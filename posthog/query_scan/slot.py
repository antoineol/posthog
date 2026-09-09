"""The scan slot: what the analysis job found for one query, keyed by its cache key.

The slot lives in the same Redis as the query cache. It is written once by the job and read
by every response served for that cache key, so an unchanged query is analyzed once a month
however often it is refreshed.

Neither a read nor a write may change what the person gets. A read returns None on any Redis
or JSON failure, and a write logs and swallows.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import structlog

from posthog.schema import QueryScanRange, QueryScanStatus, QueryScanWarning

from posthog.dataclasses import frozen
from posthog.query_cache.storage import query_cache_raw_client

logger = structlog.get_logger(__name__)

# Bumped when a stored slot can no longer be read by this code. An older version reads as
# "no slot", so the next slow run re-analyzes instead of serving a value we cannot parse.
SLOT_VERSION = 1

PENDING_TTL_SECONDS = 10 * 60
DONE_TTL_SECONDS = 30 * 24 * 60 * 60


@frozen
class QueryScanSlot:
    """One analysis of one query. ``pending`` carries only the enqueue time."""

    status: QueryScanStatus
    enqueued_at: str | None = None
    analyzed_at: str | None = None
    query_kind: str | None = None
    rows_read: int | None = None
    duration_ms: int | None = None
    events_in_range: int | None = None
    range: QueryScanRange | None = None
    explain_ok: bool | None = None
    findings: tuple[QueryScanWarning, ...] = ()
    killed: bool = False
    error_type: str | None = None


def slot_key(team_id: int, cache_key: str) -> str:
    return f"query_scan:{team_id}:{cache_key}"


def get(team_id: int, cache_key: str) -> QueryScanSlot | None:
    try:
        # The primary, not the read replica the query cache reads through. The response that
        # enqueues a scan reads the slot back in the same request, and the skip test that stops a
        # second job reads what an earlier run wrote. A replica behind the write drops both.
        raw = query_cache_raw_client().get(slot_key(team_id, cache_key))
        if raw is None:
            return None
        return _deserialize(json.loads(raw))
    except Exception:
        logger.warning("query_scan_slot_read_failed", team_id=team_id, exc_info=True)
        return None


def set_pending(team_id: int, cache_key: str) -> None:
    _write(
        team_id,
        cache_key,
        {"status": "pending", "enqueued_at": _now()},
        PENDING_TTL_SECONDS,
    )


def set_done(team_id: int, cache_key: str, slot: QueryScanSlot) -> None:
    _write(team_id, cache_key, _serialize(slot), DONE_TTL_SECONDS)


def _write(team_id: int, cache_key: str, value: dict[str, Any], ttl_seconds: int) -> None:
    try:
        payload = json.dumps({"version": SLOT_VERSION, **value})
        query_cache_raw_client().set(slot_key(team_id, cache_key), payload, ex=ttl_seconds)
    except Exception:
        logger.warning("query_scan_slot_write_failed", team_id=team_id, exc_info=True)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _serialize(slot: QueryScanSlot) -> dict[str, Any]:
    value: dict[str, Any] = {
        "status": str(slot.status),
        "analyzed_at": slot.analyzed_at or _now(),
        "query_kind": slot.query_kind,
        "rows_read": slot.rows_read,
        "duration_ms": slot.duration_ms,
        "events_in_range": slot.events_in_range,
        "range": slot.range.model_dump(by_alias=True) if slot.range is not None else None,
        "explain_ok": slot.explain_ok,
        "findings": [finding.model_dump(by_alias=True, exclude_none=True) for finding in slot.findings],
    }
    if slot.killed:
        value["killed"] = True
    if slot.error_type is not None:
        value["error_type"] = slot.error_type
    return value


def _deserialize(value: Any) -> QueryScanSlot | None:
    if not isinstance(value, dict) or value.get("version") != SLOT_VERSION:
        return None
    status = value.get("status")
    if status not in (QueryScanStatus.PENDING, QueryScanStatus.DONE):
        return None
    scan_range = value.get("range")
    findings = value.get("findings")
    return QueryScanSlot(
        status=QueryScanStatus(status),
        enqueued_at=value.get("enqueued_at"),
        analyzed_at=value.get("analyzed_at"),
        query_kind=value.get("query_kind"),
        rows_read=value.get("rows_read"),
        duration_ms=value.get("duration_ms"),
        events_in_range=value.get("events_in_range"),
        range=QueryScanRange.model_validate(scan_range) if isinstance(scan_range, dict) else None,
        explain_ok=value.get("explain_ok"),
        findings=tuple(QueryScanWarning.model_validate(finding) for finding in findings)
        if isinstance(findings, list)
        else (),
        killed=bool(value.get("killed", False)),
        error_type=value.get("error_type"),
    )
