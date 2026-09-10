"""Decide whether a finished run gets analyzed, and enqueue the job when it does.

The runner calls this once per blocking run, so everything here is cheap: the flag is already
cached in-process, and the Redis read for an existing slot happens only after the run has
passed every other test.

Nothing here may change what the person gets. The analysis is advice, so a broker or Redis
failure drops the enqueue and reports a skip, never the query result.
"""

from __future__ import annotations

from typing import Any, Literal, TypeGuard

import structlog
from pydantic import BaseModel

from posthog.schema import HogQLQueryModifiers

from posthog.hogql.query_stats import QueryStats

from posthog.clickhouse.query_tagging import get_query_tag_value, is_api_key_access_method
from posthog.dataclasses import frozen
from posthog.models.user import User
from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.slot import (
    claim_enqueue_budget,
    clear as clear_slot,
    get as get_slot,
    set_pending,
)

logger = structlog.get_logger(__name__)

SkipReason = Literal[
    "flag_off",
    "below_floor",
    "api_key",
    "no_principal",
    "not_cacheable",
    "direct_connection",
    "rate_limited",
    "slot_exists",
    "enqueue_failed",
]


@frozen
class QueryScanTrigger:
    triggered: bool
    skipped_reason: SkipReason | None


FLAG_OFF = QueryScanTrigger(triggered=False, skipped_reason="flag_off")
NO_PRINCIPAL = QueryScanTrigger(triggered=False, skipped_reason="no_principal")


def is_analyzable_principal(user: object) -> TypeGuard[User]:
    """Whether the job can rebuild the run as the person who made it.

    Only a real user row survives the trip to the worker. A shared-link viewer, a service token
    and a run with no principal all resolve to no user there, and a userless context fails closed
    on every warehouse table, so the rebuild would be narrower than the run. The scan also
    describes the project's own data volume, which is why these principals get no summary on
    their response either.
    """
    return isinstance(user, User)


def _task_payload(model: BaseModel) -> dict[str, Any]:
    """The model as JSON the job can validate back into the same model.

    Every nested `kind` discriminator has to survive: a dump that drops defaults strips them, and
    a series without them no longer parses.
    """
    return model.model_dump(mode="json", by_alias=True, exclude_none=True)


def maybe_trigger_query_scan(
    *,
    flag: QueryScanFlag | None,
    stats: QueryStats | None,
    team_id: int,
    cache_key: str,
    query: BaseModel,
    modifiers: HogQLQueryModifiers | None,
    insight_id: int | None,
    dashboard_id: int | None,
    trigger: str,
    user: User | None,
    cacheable: bool,
    killed: bool = False,
    error_type: str | None = None,
) -> QueryScanTrigger:
    """Enqueue the analysis job for this run, unless one of the skip tests holds.

    `query` and `modifiers` are serialized only on the enqueue path, because the runner calls
    this for every blocking run and most of them stop at the floor.
    """
    if flag is None or stats is None:
        return FLAG_OFF

    duration_ms = round(stats.duration_ms)
    if duration_ms < flag.floor_ms:
        return QueryScanTrigger(triggered=False, skipped_reason="below_floor")
    if is_api_key_access_method(get_query_tag_value("access_method")):
        # An API caller has no surface to read the advice on, so the analysis would only cost.
        return QueryScanTrigger(triggered=False, skipped_reason="api_key")
    if not is_analyzable_principal(user):
        return NO_PRINCIPAL
    if getattr(query, "connectionId", None):
        # A direct connection reads the external warehouse instead of ClickHouse, so the job
        # would park a pending slot for an analysis that cannot happen.
        return QueryScanTrigger(triggered=False, skipped_reason="direct_connection")
    if not cacheable:
        return QueryScanTrigger(triggered=False, skipped_reason="not_cacheable")
    if get_slot(team_id, cache_key, thresholds=flag.thresholds_fingerprint) is not None:
        return QueryScanTrigger(triggered=False, skipped_reason="slot_exists")
    if not claim_enqueue_budget(team_id):
        return QueryScanTrigger(triggered=False, skipped_reason="rate_limited")
    if not set_pending(team_id, cache_key, killed=killed):
        # Another slow run of the same query claimed the slot between the read above and here.
        return QueryScanTrigger(triggered=False, skipped_reason="slot_exists")

    # A module-level import would close the runner, trigger, task, job, runner cycle.
    from posthog.tasks.query_scan import analyze_query_scan  # noqa: PLC0415

    try:
        analyze_query_scan.delay(
            team_id=team_id,
            cache_key=cache_key,
            query=_task_payload(query),
            modifiers=_task_payload(modifiers) if modifiers is not None else None,
            insight_id=insight_id,
            dashboard_id=dashboard_id,
            rows_read=stats.rows_read,
            duration_ms=duration_ms,
            trigger=trigger,
            user_id=user.id,
            killed=killed,
            error_type=error_type,
        )
    except Exception:
        # The broker can be down while ClickHouse is fine, and the result is not cached yet, so
        # failing here would throw away a run the person already waited for.
        logger.warning("query_scan_enqueue_failed", team_id=team_id, exc_info=True)
        # Left in place, the claim above reports a pending analysis no job is coming to fill.
        clear_slot(team_id, cache_key)
        return QueryScanTrigger(triggered=False, skipped_reason="enqueue_failed")
    return QueryScanTrigger(triggered=True, skipped_reason=None)
