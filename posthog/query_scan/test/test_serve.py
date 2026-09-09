import json
from types import SimpleNamespace

from posthog.test.base import BaseTest
from unittest import mock

from parameterized import parameterized

from posthog.schema import QueryScanSummary

from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.serve import attach_scan_slot, scan_summary_with_findings

SHOW = QueryScanFlag(mode="show", floor_ms=1000, event_ratio=0.1, persons_ratio=0.5)
LOG_ONLY = QueryScanFlag(mode="log_only", floor_ms=1000, event_ratio=0.1, persons_ratio=0.5)
RAISED_FLOOR = QueryScanFlag(mode="show", floor_ms=60_000, event_ratio=0.1, persons_ratio=0.5)


class TestServeScanSummary(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        redis = mock.Mock()
        redis.get.return_value = json.dumps(
            {"version": 1, "status": "pending", "thresholds": SHOW.thresholds_fingerprint}
        )
        patcher = mock.patch("posthog.query_scan.slot.query_cache_raw_client", return_value=redis)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _cached_summary(self) -> dict:
        # What a run stored while the flag said "show" and the floor was a second.
        return {"mode": "show", "rows_read": 41_200, "duration_ms": 19_000, "status": "pending"}

    @parameterized.expand(
        [
            ("the flag went off", None, None),
            ("the mode was downgraded", LOG_ONLY, {"mode": "log_only", "status": "pending"}),
            ("the floor was raised", RAISED_FLOOR, {"mode": "show", "status": None}),
            ("nothing moved", SHOW, {"mode": "show", "status": "pending"}),
        ]
    )
    def test_a_cached_summary_is_corrected_against_the_live_flag(self, _name, flag, expected) -> None:
        with mock.patch("posthog.query_scan.serve.get_query_scan_flag", return_value=flag):
            folded = scan_summary_with_findings(self.team, self._cached_summary(), "cache_key_1")

        if expected is None:
            assert folded is None
            return
        assert folded is not None
        assert {key: folded[key] for key in expected} == expected

    def test_a_response_loses_its_summary_when_the_flag_goes_off(self) -> None:
        response = SimpleNamespace(
            query_scan=QueryScanSummary(mode="show", rows_read=41_200, duration_ms=19_000, status="pending"),
            cache_key="cache_key_1",
            warnings=[],
        )

        with mock.patch("posthog.query_scan.serve.get_query_scan_flag", return_value=None):
            attach_scan_slot(self.team, response)

        assert response.query_scan is None
