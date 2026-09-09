"""Finalize a signal handoff after its implementation task settles."""

from datetime import timedelta
from hashlib import sha256

import temporalio
from temporalio import workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.common import RetryPolicy

from posthog.dataclasses import frozen
from posthog.sync import database_sync_to_async
from posthog.temporal.common.client import async_connect
from posthog.temporal.common.scoped import scoped_temporal
from posthog.temporal.common.utils import close_db_connections

from products.signals.backend.signal_handoffs import add_task_cost, publish_handoff, read_handoff, write_handoff
from products.tasks.backend.facade import api as tasks_facade
from products.tasks.backend.facade.billing import get_task_spend


@frozen
class SignalImplementationInput:
    team_id: int
    signal_key: str
    task_id: str | None = None
    run_id: str | None = None
    additional_signal_keys: tuple[str, ...] = ()


def _signal_keys(input: SignalImplementationInput) -> tuple[str, ...]:
    return (input.signal_key, *input.additional_signal_keys)


@temporalio.activity.defn
@scoped_temporal()
@close_db_connections
async def check_implementation_task_workflow_closed_activity(input: SignalImplementationInput) -> bool:
    if not input.run_id:
        return True
    run = await database_sync_to_async(tasks_facade.get_task_run, thread_sensitive=False)(input.run_id, input.team_id)
    if run is None:
        raise ValueError(f"Implementation task run {input.run_id} is missing")
    if not run.workflow_id:
        raise ValueError(f"Implementation task run {input.run_id} has no workflow")
    client = await async_connect()
    description = await client.get_workflow_handle(run.workflow_id).describe()
    return description.status in {
        WorkflowExecutionStatus.COMPLETED,
        WorkflowExecutionStatus.FAILED,
        WorkflowExecutionStatus.CANCELED,
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
    }


@temporalio.activity.defn
@scoped_temporal()
@close_db_connections
async def finalize_signal_implementation_activity(input: SignalImplementationInput) -> None:
    if input.run_id:
        run = await database_sync_to_async(tasks_facade.get_task_run, thread_sensitive=False)(
            input.run_id, input.team_id
        )
        if run is None:
            raise ValueError(f"Implementation task run {input.run_id} is missing")
        task_id = input.task_id or str(run.task_id)
        spend = await database_sync_to_async(get_task_spend, thread_sensitive=False)(input.team_id, task_id)
        handoff = await read_handoff(input.signal_key, input.team_id)
        add_task_cost(handoff, task_id, spend, "implementation")
        await write_handoff(handoff)

    for signal_key in _signal_keys(input):
        await publish_handoff(signal_key, input.team_id)


@temporalio.workflow.defn(name="signal-implementation-finalizer")
class SignalImplementationFinalizerWorkflow:
    @staticmethod
    def workflow_id_for(team_id: int, signal_key: str, additional_signal_keys: tuple[str, ...] = ()) -> str:
        if not additional_signal_keys:
            return f"signals-implementation-finalizer:{team_id}:{signal_key}"
        keys = sorted((signal_key, *additional_signal_keys))
        key_hash = sha256("\0".join(keys).encode()).hexdigest()[:16]
        return f"signals-implementation-finalizer:{team_id}:batch:{key_hash}"

    @temporalio.workflow.run
    async def run(self, input: SignalImplementationInput) -> None:
        if input.run_id:
            while not await workflow.execute_activity(
                check_implementation_task_workflow_closed_activity,
                input,
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(maximum_attempts=3),
            ):
                await workflow.sleep(timedelta(seconds=60))
        await workflow.execute_activity(
            finalize_signal_implementation_activity,
            input,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
