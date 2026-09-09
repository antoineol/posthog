from datetime import datetime
from pathlib import Path
from typing import Any

from posthog.test.base import BaseTest
from unittest import mock

from posthog.query_scan import slot
from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.job import QueryScanJob, run_query_scan

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
        redis.set.side_effect = lambda key, value, ex=None: self.stored.__setitem__(key, value)
        for client in ("query_cache_read_client", "query_cache_raw_client"):
            patcher = mock.patch(f"posthog.query_scan.slot.{client}", return_value=redis)
            patcher.start()
            self.addCleanup(patcher.stop)

        flag_patcher = mock.patch("posthog.query_scan.job.get_query_scan_flag", return_value=FLAG)
        flag_patcher.start()
        self.addCleanup(flag_patcher.stop)

        capture_patcher = mock.patch("posthog.query_scan.job.ph_scoped_capture")
        self.capture = capture_patcher.start().return_value.__enter__.return_value
        self.addCleanup(capture_patcher.stop)

    def _job(self) -> QueryScanJob:
        return QueryScanJob(
            team=self.team,
            user=self.user,
            cache_key="cache_key_1",
            query={"kind": "HogQLQuery", "query": "select count() from events"},
            modifiers=None,
            insight_id=None,
            dashboard_id=None,
            rows_read=ROWS_READ,
            duration_ms=19_000,
            trigger="fresh",
        )

    def _run(self, *, explain: Any = None, count_error: Exception | None = None) -> None:
        plan = (FIXTURES / "no_event_filter.json").read_text() if explain is None else explain

        def execute(query: str, *args: Any, **kwargs: Any) -> Any:
            if query.startswith("EXPLAIN"):
                if isinstance(plan, Exception):
                    raise plan
                return [[plan]]
            if "min(timestamp)" in query:
                if count_error is not None:
                    raise count_error
                return [(EVENTS_IN_RANGE, datetime(2025, 8, 5))]
            return [(200_000,)]

        with mock.patch("posthog.query_scan.job.sync_execute", side_effect=execute):
            run_query_scan(self._job())

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
