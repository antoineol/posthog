"""What the query scan job does, away from Celery so it can be called directly in a test.

The job rebuilds the query the person ran, asks ClickHouse how it planned to read it, counts
the project's events over the same range, runs the structural checks, and stores the result in
the scan slot. It runs on the offline pool, once per query per slot lifetime.

Every ClickHouse read goes through ``sync_execute`` here, so one patch covers the whole job.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from time import perf_counter
from typing import Any

import structlog

from posthog.schema import HogQLFilters, HogQLQueryModifiers, QueryScanRange, QueryScanStatus

from posthog.hogql.constants import LimitContext
from posthog.hogql.context import HogQLContext
from posthog.hogql.parser import parse_select
from posthog.hogql.placeholders import find_placeholders
from posthog.hogql.query import HogQLQueryExecutor

from posthog.clickhouse.client import sync_execute
from posthog.clickhouse.client.connection import Workload
from posthog.clickhouse.query_tagging import Feature, Product, tags_context
from posthog.dataclasses import frozen
from posthog.event_usage import groups
from posthog.exceptions_capture import capture_exception
from posthog.hogql_queries.hogql_query_runner import HogQLQueryRunner
from posthog.hogql_queries.query_runner import QueryRunner, get_query_runner
from posthog.models.team.team import Team
from posthog.models.user import User
from posthog.ph_client import ph_scoped_capture
from posthog.query_scan.analyze import QueryScanResult, ScanRange, ScanThresholds, analyze, analyze_settings
from posthog.query_scan.checks.persons import check_persons_join
from posthog.query_scan.checks.start_date import check_start_date
from posthog.query_scan.explain import QueryPlan, parse_query_plan
from posthog.query_scan.flag import get_query_scan_flag
from posthog.query_scan.slot import QueryScanSlot, set_done
from posthog.query_scan.tree import find_events_reads

logger = structlog.get_logger(__name__)

# Every ClickHouse read this job makes carries it. `sync_execute` sets no cap of its own, and
# the production socket waits effectively forever, so without this a statement ClickHouse does
# not return holds a slot on a queue sized to protect ClickHouse.
MAX_EXECUTION_TIME_SECONDS = 60


@frozen
class QueryScanJob:
    """One run to analyze, as the runner saw it."""

    team: Team
    user: User | None
    cache_key: str
    query: dict[str, Any]
    modifiers: dict[str, Any] | None
    insight_id: int | None
    dashboard_id: int | None
    rows_read: int
    duration_ms: int
    trigger: str
    killed: bool = False
    error_type: str | None = None


@frozen
class _EventCount:
    events: int | None
    min_timestamp: datetime | None = None


@frozen(eq=False)
class _Analysis:
    result: QueryScanResult
    events_in_range: int | None
    person_rows: int | None = None
    min_timestamp: datetime | None = None


def run_query_scan(job: QueryScanJob) -> None:
    """Analyze one run and store the result. Never raises, and never retries: the next slow
    run of the same query enqueues a new job."""
    started = perf_counter()
    try:
        _run(job, started)
    except Exception as error:
        capture_exception(
            error,
            {"team_id": job.team.pk, "cache_key": job.cache_key, "context": "query_scan_job"},
        )


def _run(job: QueryScanJob, started: float) -> None:
    flag = get_query_scan_flag(job.team)
    if flag is None:
        # The flag went off between the enqueue and now. Leave the pending slot to expire.
        return
    thresholds = ScanThresholds(event_ratio=flag.event_ratio, persons_ratio=flag.persons_ratio)

    modifiers = HogQLQueryModifiers.model_validate(job.modifiers) if job.modifiers else None
    runner = get_query_runner(job.query, job.team, modifiers=modifiers, user=job.user)
    # A direct connection reads the external warehouse, not ClickHouse, and the Trino engine is
    # only reachable through one, so there is nothing here to explain or count.
    if getattr(runner.query, "connectionId", None):
        return

    kind = getattr(runner.query, "kind", None)
    query_kind = str(kind) if kind is not None else None
    if isinstance(runner, HogQLQueryRunner):
        analysis = _analyze_hogql(runner, job, thresholds)
    else:
        analysis = _analyze_settings(runner, job, thresholds)
    if analysis is None:
        return

    set_done(
        job.team.pk,
        job.cache_key,
        QueryScanSlot(
            status=QueryScanStatus.DONE,
            analyzed_at=datetime.now(UTC).isoformat(),
            query_kind=query_kind,
            rows_read=job.rows_read,
            duration_ms=job.duration_ms,
            events_in_range=analysis.events_in_range,
            range=_slot_range(analysis.result.range, analysis.min_timestamp),
            explain_ok=analysis.result.explain_ok,
            findings=tuple(analysis.result.findings),
            killed=job.killed,
            error_type=job.error_type,
            thresholds=flag.thresholds_fingerprint,
        ),
    )
    _report(job, analysis, query_kind=query_kind, job_ms=round((perf_counter() - started) * 1000))


def _analyze_hogql(runner: HogQLQueryRunner, job: QueryScanJob, thresholds: ScanThresholds) -> _Analysis | None:
    executor = HogQLQueryExecutor(
        query=runner.to_query(),
        team=job.team,
        query_type="HogQLQuery",
        filters=runner.query.filters,
        variables=runner.query.variables,
        modifiers=runner.modifiers,
        user=job.user,
        user_access_control=runner.user_access_control,
        limit_context=LimitContext.QUERY_ASYNC,
        context=runner.build_hogql_context(),
    )
    sql, context = executor.generate_clickhouse_sql()
    prepared_tree = executor.clickhouse_prepared_ast
    clickhouse_context = executor.clickhouse_context
    if prepared_tree is None or clickhouse_context is None:
        return None

    plan = _explain(sql, context, job.team)
    has_filters_placeholder = _has_open_filters_placeholder(runner.query.query, runner.query.filters)
    start_date = check_start_date(prepared_tree, has_filters_placeholder=has_filters_placeholder)
    # A query that reads no events has no denominator to measure against: every check that
    # would use one is already quiet, and the project's whole event count is not what such a
    # query read, so counting it would only store a ratio that means nothing.
    reads_events = bool(find_events_reads(prepared_tree))
    counted = (
        _count_events_in_range(job.team, start_date.date_from, start_date.date_to)
        if reads_events
        else _EventCount(events=None)
    )
    # The person count only decides the gate on an unfiltered join, so a query that pushes a
    # filter into the subquery, or never joins persons at all, skips it.
    person_rows = _count_person_rows(job.team) if check_persons_join(clickhouse_context).unfiltered else None

    result = analyze(
        prepared_tree,
        clickhouse_context,
        plan=plan,
        rows_read=job.rows_read,
        duration_ms=job.duration_ms,
        killed=job.killed,
        events_in_range=counted.events,
        min_timestamp=counted.min_timestamp,
        person_rows=person_rows,
        has_filters_placeholder=has_filters_placeholder,
        thresholds=thresholds,
    )
    return _Analysis(
        result=result,
        events_in_range=counted.events,
        person_rows=person_rows,
        min_timestamp=counted.min_timestamp,
    )


def _analyze_settings(runner: QueryRunner, job: QueryScanJob, thresholds: ScanThresholds) -> _Analysis:
    resolved = _resolved_date_range(runner)
    date_from, date_to = resolved.date_from, resolved.date_to
    # Without a resolved range there is nothing to measure the read against, and the only check
    # that would still fire needs a date range on the insight to fire at all.
    counted = _count_events_in_range(job.team, date_from, date_to) if date_to is not None else _EventCount(events=None)

    result = analyze_settings(
        runner.query.model_dump(mode="json"),
        date_from=date_from,
        date_to=date_to,
        rows_read=job.rows_read,
        duration_ms=job.duration_ms,
        killed=job.killed,
        events_in_range=counted.events,
        thresholds=thresholds,
    )
    return _Analysis(result=result, events_in_range=counted.events, min_timestamp=counted.min_timestamp)


@frozen
class _ResolvedDateRange:
    date_from: date | None
    date_to: date | None


def _resolved_date_range(runner: QueryRunner) -> _ResolvedDateRange:
    """The dates the runner resolved for the insight, which is where a picker-built query says
    what it covers. Runners without one report no range."""
    query_date_range = getattr(runner, "query_date_range", None)
    if query_date_range is None:
        return _ResolvedDateRange(date_from=None, date_to=None)
    return _ResolvedDateRange(date_from=query_date_range.date_from().date(), date_to=query_date_range.date_to().date())


def _has_open_filters_placeholder(query: str, filters: HogQLFilters | None) -> bool:
    """Whether the query asks for a date range through ``{filters}`` and nobody supplied one.

    The placeholder then expands to no bound at all, so the missing start date is on the insight
    or the dashboard rather than in the SQL, and the advice has to say so. Only the predicate
    forms count: ``{filters.interval(...)}`` and ``{filters.breakdown(...)}`` substitute a value,
    so no date range on the insight can bound the query through them.
    """
    try:
        if not find_placeholders(parse_select(query)).has_date_filters:
            return False
    except Exception:
        return False
    date_range = filters.dateRange if filters else None
    if date_range is None:
        return True
    # "all" promises the whole table, so the placeholder still expands to no lower bound.
    date_from = None if date_range.date_from == "all" else date_range.date_from
    return not (date_from or date_range.date_to)


def _explain(sql: str, context: HogQLContext, team: Team) -> QueryPlan | None:
    """The plan ClickHouse would use, or None when EXPLAIN fails. The checks then use the tree
    alone, which cannot see a filter ClickHouse dropped from the key condition."""
    try:
        with tags_context(product=Product.PRODUCT_ANALYTICS, feature=Feature.QUERY_SCAN):
            # nosemgrep: clickhouse-fstring-param-audit - sql is compiled from the HogQL AST by the printer, and its values stay parameterized in context.values
            rows = sync_execute(
                f"EXPLAIN indexes = 1, json = 1 {sql}",
                context.values,
                settings={"max_execution_time": MAX_EXECUTION_TIME_SECONDS},
                workload=Workload.OFFLINE,
                team_id=team.pk,
                readonly=True,
                external_tables=list(context.external_tables.values()) or None,
            )
        return parse_query_plan(rows[0][0])
    except Exception:
        logger.warning("query_scan_explain_failed", team_id=team.pk, exc_info=True)
        return None


def _count_events_in_range(team: Team, date_from: date | None, date_to: date | None) -> _EventCount:
    """How many events the project holds over the range the query read.

    The events table is sorted by team and day, so this reads the key columns for the project's
    granules in range and nothing else. A missing bound is left out of the filter, and the
    earliest timestamp stands in for a missing lower bound in the copy.
    """
    conditions = ["team_id = %(team_id)s"]
    arguments: dict[str, Any] = {"team_id": team.pk}
    if date_from is not None:
        conditions.append("timestamp >= %(date_from)s")
        arguments["date_from"] = date_from
    if date_to is not None:
        conditions.append("timestamp < %(date_to)s")
        # The range is rounded out to whole days, so the exclusive bound is the day after it.
        arguments["date_to"] = date_to + timedelta(days=1)

    # Every piece of the statement is a literal; the bounds travel as parameters.
    sql = "SELECT count(), min(timestamp) FROM events WHERE " + " AND ".join(conditions)
    with tags_context(product=Product.PRODUCT_ANALYTICS, feature=Feature.QUERY_SCAN):
        rows = sync_execute(
            sql,
            arguments,
            settings={"max_execution_time": MAX_EXECUTION_TIME_SECONDS},
            workload=Workload.OFFLINE,
            team_id=team.pk,
            readonly=True,
        )
    count, earliest = rows[0]
    # min() over no rows returns the zero date, which would read as data from 1970.
    return _EventCount(events=count, min_timestamp=earliest if count else None)


def _count_person_rows(team: Team) -> int:
    """Person versions for the project, which is what the deduplicating subquery reads."""
    with tags_context(product=Product.PRODUCT_ANALYTICS, feature=Feature.QUERY_SCAN):
        rows = sync_execute(
            "SELECT count() FROM person WHERE team_id = %(team_id)s",
            {"team_id": team.pk},
            settings={"max_execution_time": MAX_EXECUTION_TIME_SECONDS},
            workload=Workload.OFFLINE,
            team_id=team.pk,
            readonly=True,
        )
    return rows[0][0]


def _slot_range(scan_range: ScanRange | None, min_timestamp: datetime | None) -> QueryScanRange | None:
    if scan_range is None or scan_range.date_to is None:
        return None
    # With no start date the count covered everything the project has, so the earliest event it
    # found is the range it actually read.
    date_from = scan_range.date_from or (min_timestamp.date() if min_timestamp is not None else None)
    if date_from is None:
        return None
    return QueryScanRange.model_validate({"from": date_from.isoformat(), "to": scan_range.date_to.isoformat()})


def _report(job: QueryScanJob, analysis: _Analysis, *, query_kind: str | None, job_ms: int) -> None:
    """Send `query scan analyzed`, findings or not. A run with no findings is the record of an
    expensive query no check explains, which is what says which check to write next."""
    result = analysis.result
    properties = {
        "cache_key": job.cache_key,
        "insight_id": job.insight_id,
        "dashboard_id": job.dashboard_id,
        "query_kind": query_kind,
        "trigger": job.trigger,
        "rows_read": job.rows_read,
        "duration_ms": job.duration_ms,
        "events_in_range": analysis.events_in_range,
        "event_ratio": result.event_ratio,
        "person_rows": analysis.person_rows,
        "explain_ok": result.explain_ok,
        "finding_kinds": result.finding_kinds(),
        "event_filter_class": result.event_filter_class,
        "start_date_class": result.start_date_class,
        "killed": job.killed,
        "error_type": job.error_type,
        "job_ms": job_ms,
    }
    distinct_id = job.user.distinct_id if job.user and job.user.distinct_id else str(job.team.uuid)
    # A Celery worker can exit before the global client's background flush runs, so this event
    # needs a client that is flushed here.
    with ph_scoped_capture() as capture:
        capture(
            distinct_id=distinct_id,
            event="query scan analyzed",
            properties=properties,
            groups=groups(job.team.organization, job.team),
        )
