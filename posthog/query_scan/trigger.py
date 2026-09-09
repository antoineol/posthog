"""Decide whether a finished run gets analyzed, and enqueue the job when it does.

The runner calls this once per blocking run, so everything here is cheap: the flag is already
cached in-process, and the Redis read for an existing slot happens only after the run has
passed every other test.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from posthog.schema import HogQLQueryModifiers

from posthog.hogql.query_stats import QueryStats

from posthog.clickhouse.query_tagging import get_query_tag_value, is_api_key_access_method
from posthog.dataclasses import frozen
from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.slot import (
    get as get_slot,
    set_pending,
)

SkipReason = Literal["flag_off", "below_floor", "api_key", "not_cacheable", "slot_exists"]


@frozen
class QueryScanTrigger:
    """Whether the run was enqueued for analysis, and the reason when it was not."""

    triggered: bool
    skipped_reason: SkipReason | None


FLAG_OFF = QueryScanTrigger(triggered=False, skipped_reason="flag_off")


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
    user_id: int | None,
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
        # An API caller has no surface to read the advice on, so analyzing costs without paying.
        return QueryScanTrigger(triggered=False, skipped_reason="api_key")
    if not cacheable:
        return QueryScanTrigger(triggered=False, skipped_reason="not_cacheable")
    if get_slot(team_id, cache_key) is not None:
        return QueryScanTrigger(triggered=False, skipped_reason="slot_exists")

    # The query runner imports this module, and the task's job imports the query runner, so a
    # module-level import here would close that cycle.
    from posthog.tasks.query_scan import analyze_query_scan  # noqa: PLC0415

    set_pending(team_id, cache_key)
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
        user_id=user_id,
        killed=killed,
        error_type=error_type,
    )
    return QueryScanTrigger(triggered=True, skipped_reason=None)
