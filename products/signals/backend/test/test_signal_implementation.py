from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from products.signals.backend.signal_handoffs import SignalHandoff
from products.signals.backend.temporal.report_safety_judge import (
    SafetyJudgeInput,
    SafetyJudgeResponse,
    report_safety_judge_activity,
)
from products.signals.backend.temporal.signal_implementation import (
    SignalImplementationFinalizerWorkflow,
    SignalImplementationInput,
    check_implementation_task_workflow_closed_activity,
    finalize_signal_implementation_activity,
)
from products.signals.backend.temporal.types import SignalData


@pytest.mark.asyncio
async def test_finalizer_charges_a_task_once_when_the_finish_activity_retries() -> None:
    handoff = SignalHandoff(
        team_id=1,
        signal=SignalData(
            signal_id="signal-id",
            content="content",
            source_product="signals",
            source_type="test",
            source_id="source-id",
            weight=1.0,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )
    workflow_handle = MagicMock(
        describe=AsyncMock(return_value=SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED))
    )
    client = MagicMock(get_workflow_handle=MagicMock(return_value=workflow_handle))
    run = SimpleNamespace(workflow_id="task-workflow", task_id="task-id")
    spend = SimpleNamespace(token_cost=10, compute_cost=20)

    def async_boundary(fn, **_kwargs):
        async def call(*args, **kwargs):
            return fn(*args, **kwargs)

        return call

    with (
        patch("products.signals.backend.temporal.signal_implementation.database_sync_to_async", async_boundary),
        patch("products.signals.backend.temporal.signal_implementation.tasks_facade.get_task_run", return_value=run),
        patch("products.signals.backend.temporal.signal_implementation.get_task_spend", return_value=spend),
        patch("products.signals.backend.temporal.signal_implementation.async_connect", AsyncMock(return_value=client)),
        patch("products.signals.backend.temporal.signal_implementation.read_handoff", AsyncMock(return_value=handoff)),
        patch("products.signals.backend.temporal.signal_implementation.write_handoff", AsyncMock()) as write_handoff,
        patch(
            "products.signals.backend.temporal.signal_implementation.publish_handoff", AsyncMock()
        ) as publish_handoff,
    ):
        input = SignalImplementationInput(team_id=1, signal_key="handoff", task_id="task-id", run_id="run-id")
        assert await check_implementation_task_workflow_closed_activity(input)
        await finalize_signal_implementation_activity(input)
        await finalize_signal_implementation_activity(input)

    workflow_handle.describe.assert_awaited_once()
    assert handoff.costed_tasks == ["task-id"]
    assert handoff.signal.metadata["token_cost"]["implementation"] == 10
    assert handoff.signal.metadata["compute_cost"]["implementation"] == 20
    assert write_handoff.await_count == 2
    assert publish_handoff.await_count == 2


@pytest.mark.asyncio
async def test_finalizer_workflow_polls_until_closed_then_publishes() -> None:
    check_answers = iter([False, True])
    finalized_inputs: list[SignalImplementationInput] = []

    @activity.defn(name="check_implementation_task_workflow_closed_activity")
    async def check_closed(input: SignalImplementationInput) -> bool:
        return next(check_answers)

    @activity.defn(name="finalize_signal_implementation_activity")
    async def finalize(input: SignalImplementationInput) -> None:
        finalized_inputs.append(input)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="signal-implementation-test",
            workflows=[SignalImplementationFinalizerWorkflow],
            activities=[check_closed, finalize],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            await env.client.execute_workflow(
                SignalImplementationFinalizerWorkflow.run,
                SignalImplementationInput(team_id=1, signal_key="owner", run_id="run-id"),
                id="signal-implementation-finalizer-test",
                task_queue="signal-implementation-test",
            )

    assert finalized_inputs == [SignalImplementationInput(team_id=1, signal_key="owner", run_id="run-id")]


@pytest.mark.asyncio
async def test_finalizer_workflow_publishes_a_signal_batch_in_one_activity() -> None:
    signal_keys = [f"signal-{index}" for index in range(20)]
    finalized_inputs: list[SignalImplementationInput] = []

    @activity.defn(name="finalize_signal_implementation_activity")
    async def finalize(input: SignalImplementationInput) -> None:
        finalized_inputs.append(input)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="signal-implementation-batch-test",
            workflows=[SignalImplementationFinalizerWorkflow],
            activities=[finalize],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            await env.client.execute_workflow(
                SignalImplementationFinalizerWorkflow.run,
                SignalImplementationInput(
                    team_id=1,
                    signal_key=signal_keys[0],
                    additional_signal_keys=tuple(signal_keys[1:]),
                ),
                id="signal-implementation-finalizer-batch-test",
                task_queue="signal-implementation-batch-test",
            )

    assert finalized_inputs == [
        SignalImplementationInput(
            team_id=1,
            signal_key=signal_keys[0],
            additional_signal_keys=tuple(signal_keys[1:]),
        )
    ]


@pytest.mark.asyncio
async def test_safety_rejection_marks_handoff_deleted_before_it_is_saved() -> None:
    handoff = SignalHandoff(
        team_id=1,
        signal=SignalData(
            signal_id="signal-id",
            content="content",
            source_product="signals",
            source_type="test",
            source_id="source-id",
            weight=1.0,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )

    def async_boundary(fn, **_kwargs):
        async def call(*args, **kwargs):
            return fn(*args, **kwargs)

        return call

    with (
        patch("products.signals.backend.temporal.report_safety_judge.database_sync_to_async", async_boundary),
        patch(
            "products.signals.backend.temporal.report_safety_judge.judge_report_safety",
            AsyncMock(return_value=SafetyJudgeResponse(choice=False, explanation="injection")),
        ),
        patch("products.signals.backend.temporal.report_safety_judge.SignalReportArtefact.append_status"),
        patch("products.signals.backend.temporal.report_safety_judge.read_handoff", AsyncMock(return_value=handoff)),
        patch("products.signals.backend.temporal.report_safety_judge.write_handoff", AsyncMock()) as write_handoff,
    ):
        await report_safety_judge_activity(
            SafetyJudgeInput(team_id=1, report_id="report-id", signals=[], signal_key="handoff")
        )

    assert handoff.signal.metadata["deleted"] is True
    assert write_handoff.await_count == 1


@pytest.mark.asyncio
async def test_finalizer_rejects_a_missing_task_run() -> None:
    def async_boundary(fn, **_kwargs):
        async def call(*args, **kwargs):
            return fn(*args, **kwargs)

        return call

    with (
        patch("products.signals.backend.temporal.signal_implementation.database_sync_to_async", async_boundary),
        patch("products.signals.backend.temporal.signal_implementation.tasks_facade.get_task_run", return_value=None),
    ):
        with pytest.raises(ValueError, match="is missing"):
            await finalize_signal_implementation_activity(
                SignalImplementationInput(team_id=1, signal_key="handoff", task_id="task-id", run_id="run-id")
            )
