from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from posthog.test.base import BaseTest
from unittest import mock

from django.test import SimpleTestCase, override_settings

from dateutil.relativedelta import relativedelta
from parameterized import parameterized

from posthog.schema import DateRange, HogQLFilters, PersonsOnEventsMode

from posthog.query_scan import slot
from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.job import QueryScanJob, _has_open_filters_placeholder, run_query_scan

FIXTURES = Path(__file__).parent / "fixtures"

FLAG = QueryScanFlag(mode="show", floor_ms=1000, event_ratio=0.1, persons_ratio=0.5)

EVENTS_IN_RANGE = 1_000_000
ROWS_READ = 500_000


class TestQueryScanJob(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.stored: dict[str, Any] = {}
        redis = mock.Mock()
        redis.get.side_effect = lambda key: self.stored.get(key)
        redis.set.side_effect = lambda key, value, ex=None, nx=False: self.stored.__setitem__(key, value)
        patcher = mock.patch("posthog.query_scan.slot.query_cache_raw_client", return_value=redis)
        patcher.start()
        self.addCleanup(patcher.stop)

        flag_patcher = mock.patch("posthog.query_scan.job.get_query_scan_flag", return_value=FLAG)
        flag_patcher.start()
        self.addCleanup(flag_patcher.stop)

        capture_patcher = mock.patch("posthog.query_scan.job.ph_scoped_capture")
        self.capture = capture_patcher.start().return_value.__enter__.return_value
        self.addCleanup(capture_patcher.stop)

    def _job(self, sql: str = "select count() from events") -> QueryScanJob:
        return QueryScanJob(
            team=self.team,
            user=self.user,
            cache_key="cache_key_1",
            query={"kind": "HogQLQuery", "query": sql},
            modifiers=None,
            insight_id=None,
            dashboard_id=None,
            rows_read=ROWS_READ,
            duration_ms=19_000,
            trigger="fresh",
        )

    def _run(self, *, explain: Any = None, count_error: Exception | None = None, sql: str | None = None) -> list[str]:
        plan = (FIXTURES / "no_event_filter.json").read_text() if explain is None else explain
        executed: list[str] = []

        def execute(query: str, *args: Any, **kwargs: Any) -> Any:
            executed.append(query)
            if query.startswith("EXPLAIN"):
                if isinstance(plan, Exception):
                    raise plan
                return [[plan]]
            if "min(timestamp)" in query:
                if count_error is not None:
                    raise count_error
                return [(EVENTS_IN_RANGE, datetime(2025, 8, 5))]
            return [(200_000,)]

        def count_persons(query: str, **kwargs: Any) -> Any:
            executed.append(query)
            return mock.Mock(results=[[200_000]])

        with (
            mock.patch("posthog.query_scan.job.sync_execute", side_effect=execute),
            mock.patch("posthog.query_scan.job.execute_hogql_query", side_effect=count_persons),
        ):
            run_query_scan(self._job(sql=sql) if sql is not None else self._job())
        return executed

    def test_writes_a_done_slot_and_reports_it(self) -> None:
        self._run()

        stored = slot.get(self.team.pk, "cache_key_1")
        assert stored is not None
        assert stored.status == "done"
        assert stored.explain_ok is True
        assert stored.events_in_range == EVENTS_IN_RANGE
        assert stored.rows_read == ROWS_READ
        # The query names no events and has no start date, so both checks fire.
        assert {str(finding.kind) for finding in stored.findings} == {"no_event_filter", "no_start_date"}

        assert self.capture.call_count == 1
        properties = self.capture.call_args.kwargs["properties"]
        assert self.capture.call_args.kwargs["event"] == "query scan analyzed"
        assert sorted(properties["finding_kinds"]) == ["no_event_filter", "no_start_date"]
        assert properties["events_in_range"] == EVENTS_IN_RANGE
        assert properties["explain_ok"] is True

    def test_a_finding_quotes_the_clause_the_person_typed(self) -> None:
        self._run(sql="select count() from events where properties.plan = 'pro' or event = 'upgrade'")

        stored = slot.get(self.team.pk, "cache_key_1")
        assert stored is not None
        clauses = {str(finding.kind): finding.clause for finding in stored.findings}
        assert clauses["event_filter_not_used"] == "properties.plan = 'pro' or event = 'upgrade'"

    def test_a_done_slot_from_other_thresholds_reads_as_absent(self) -> None:
        self._run()

        assert slot.get(self.team.pk, "cache_key_1", thresholds=FLAG.thresholds_fingerprint) is not None
        raised = QueryScanFlag(mode="show", floor_ms=1000, event_ratio=0.9, persons_ratio=0.5)
        assert slot.get(self.team.pk, "cache_key_1", thresholds=raised.thresholds_fingerprint) is None

    def test_a_pending_slot_survives_a_threshold_change(self) -> None:
        # The job in flight reads the current gates itself, so rejecting its slot would only
        # enqueue a second one.
        slot.set_pending(self.team.pk, "cache_key_2")

        raised = QueryScanFlag(mode="show", floor_ms=1000, event_ratio=0.9, persons_ratio=0.5)
        assert slot.get(self.team.pk, "cache_key_2", thresholds=raised.thresholds_fingerprint) is not None

    @parameterized.expand(
        [
            # Reads no events and pushes the filter into the persons subquery, so neither
            # denominator has a consumer: the whole-project counts would be pure waste, and an
            # event ratio built from them would describe a table this query never touched.
            (
                "no events and a filtered persons join",
                "select count() from persons where properties.email = 'a@b.c'",
                None,
                False,
                False,
            ),
            # An unfiltered join is the one shape the persons gate reads, and it reads events too.
            (
                "events joined to every person",
                "select count() from events as e join persons as p on e.person_id = p.id where e.event = 'purchase'",
                None,
                True,
                True,
            ),
            # The persons finding tells the person to read person properties off the events
            # table, which this mode does not fill, so there is no advice the count could gate.
            (
                "an unfiltered join with persons on events off",
                "select count() from events as e join persons as p on e.person_id = p.id where e.event = 'purchase'",
                PersonsOnEventsMode.DISABLED,
                True,
                False,
            ),
        ]
    )
    def test_a_count_runs_only_when_a_check_consumes_it(
        self,
        _name: str,
        sql: str,
        person_on_events_mode: PersonsOnEventsMode | None,
        expect_events_count: bool,
        expect_person_count: bool,
    ) -> None:
        if person_on_events_mode is not None:
            self.team.modifiers = {"personsOnEventsMode": person_on_events_mode.value}
            self.team.save()

        executed = self._run(sql=sql)

        counts = [query for query in executed if not query.startswith("EXPLAIN")]
        assert any("min(timestamp)" in query for query in counts) is expect_events_count
        assert any("FROM raw_persons" in query for query in counts) is expect_person_count

    def test_the_event_count_uses_the_exact_bounds_the_query_gave(self) -> None:
        # Rounding an explicit range out to whole days would count a day the query never read and
        # understate the ratio, so the count takes the instants the bounds evaluated to.
        calls: list[tuple[str, dict[str, Any]]] = []

        def execute(query: str, arguments: Any = None, *args: Any, **kwargs: Any) -> Any:
            calls.append((query, arguments or {}))
            if query.startswith("EXPLAIN"):
                return [[(FIXTURES / "no_event_filter.json").read_text()]]
            if "min(timestamp)" in query:
                return [(EVENTS_IN_RANGE, datetime(2026, 1, 10, 8))]
            return [(200_000,)]

        with mock.patch("posthog.query_scan.job.sync_execute", side_effect=execute):
            run_query_scan(
                self._job(
                    sql="select count() from events where timestamp >= '2026-01-10 08:00:00' "
                    "and timestamp < '2026-01-12 18:30:00'"
                )
            )

        _query, arguments = next(call for call in calls if "min(timestamp)" in call[0])
        assert arguments["date_from"] == datetime(2026, 1, 10, 8, 0)
        assert arguments["date_to"] == datetime(2026, 1, 12, 18, 30)

    @override_settings(EVENTS_DATA_RETENTION_ENFORCED=True)
    def test_the_event_count_stops_at_the_retention_floor(self) -> None:
        # The count is shown to the person, and the printer floors every events scan the same
        # way, so counting past the floor would report events their query can no longer read.
        self.team.event_retention_months = 12
        self.team.save()

        executed = self._run(sql="select count() from events where timestamp > '2019-01-01'")

        count_sql = next(query for query in executed if "min(timestamp)" in query)
        assert "toIntervalMonth(%(retention_months)s)" in count_sql
        stored = slot.get(self.team.pk, "cache_key_1")
        assert stored is not None
        assert stored.range is not None
        assert stored.range.date_from == (datetime.now(UTC) - relativedelta(months=12)).date().isoformat()

    def test_a_failed_explain_still_writes_a_done_slot(self) -> None:
        self._run(explain=Exception("EXPLAIN timed out"))

        stored = slot.get(self.team.pk, "cache_key_1")
        assert stored is not None
        assert stored.status == "done"
        assert stored.explain_ok is False
        assert self.capture.call_count == 1

    def test_a_failed_count_leaves_the_pending_slot(self) -> None:
        slot.set_pending(self.team.pk, "cache_key_1")

        self._run(count_error=Exception("count timed out"))

        stored = slot.get(self.team.pk, "cache_key_1")
        assert stored is not None
        assert stored.status == "pending"
        self.capture.assert_not_called()


class TestOpenFiltersPlaceholder(SimpleTestCase):
    @parameterized.expand(
        [
            ("no placeholder at all", "select count() from events", None, False),
            ("open filters", "select count() from events where {filters}", None, True),
            ("open bound filters", "select count() from events where {filters(timestamp AS timestamp)}", None, True),
            (
                "filters with a date range supplied",
                "select count() from events where {filters}",
                HogQLFilters(dateRange=DateRange(date_from="-7d")),
                False,
            ),
            (
                "filters set to all time",
                "select count() from events where {filters}",
                HogQLFilters(dateRange=DateRange(date_from="all")),
                True,
            ),
            (
                "only an interval placeholder",
                "select toStartOfInterval(timestamp, {filters.interval('day')}), count() from events group by 1",
                None,
                False,
            ),
            (
                "only a breakdown placeholder",
                "select {filters.breakdown(properties.plan AS 'plan')}, count() from events group by 1",
                None,
                False,
            ),
        ]
    )
    def test_only_a_date_carrying_placeholder_puts_the_start_date_on_the_insight(
        self, _name: str, query: str, filters: HogQLFilters | None, expected: bool
    ) -> None:
        assert _has_open_filters_placeholder(query, filters) is expected
