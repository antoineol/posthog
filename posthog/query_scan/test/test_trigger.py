import json
from typing import Any

from unittest import mock

from django.test import SimpleTestCase

from parameterized import parameterized

from posthog.schema import EventsNode, FunnelsQuery, HogQLQuery, TrendsQuery

from posthog.hogql.query_stats import QueryStats

from posthog.clickhouse.query_tagging import AccessMethod, reset_query_tags, tag_queries
from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.trigger import maybe_trigger_query_scan

FLAG = QueryScanFlag(mode="show", floor_ms=1000, event_ratio=0.1, persons_ratio=0.5)


def _tag_as_api_key(test: "TestQueryScanTrigger") -> None:
    tag_queries(access_method=AccessMethod.PERSONAL_API_KEY)


def _store_a_slot(test: "TestQueryScanTrigger", thresholds: str = FLAG.thresholds_fingerprint) -> None:
    test.redis.get.return_value = json.dumps({"version": 1, "status": "done", "findings": [], "thresholds": thresholds})


class TestQueryScanTrigger(SimpleTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.redis = mock.Mock()
        self.redis.get.return_value = None
        patcher = mock.patch("posthog.query_scan.slot.query_cache_raw_client", return_value=self.redis)
        patcher.start()
        self.addCleanup(patcher.stop)
        delay_patcher = mock.patch("posthog.tasks.query_scan.analyze_query_scan.delay")
        self.delay = delay_patcher.start()
        self.addCleanup(delay_patcher.stop)
        self.addCleanup(reset_query_tags)

    def _trigger(self, **overrides: Any):
        arguments: dict[str, Any] = {
            "flag": FLAG,
            "stats": QueryStats(rows_read=10, duration_ms=2000.0),
            "team_id": 1,
            "cache_key": "cache_key_1",
            "query": HogQLQuery(query="select 1"),
            "modifiers": None,
            "insight_id": None,
            "dashboard_id": None,
            "trigger": "fresh",
            "user_id": None,
            "cacheable": True,
        }
        return maybe_trigger_query_scan(**{**arguments, **overrides})

    @parameterized.expand(
        [
            ("flag off", {"flag": None}, None, "flag_off"),
            ("below the floor", {"stats": QueryStats(rows_read=10, duration_ms=999.0)}, None, "below_floor"),
            ("api key run", {}, _tag_as_api_key, "api_key"),
            ("result not cacheable", {"cacheable": False}, None, "not_cacheable"),
            ("slot already exists", {}, _store_a_slot, "slot_exists"),
        ]
    )
    def test_skips_with_a_reason(self, _name, overrides, prepare, expected_reason) -> None:
        if prepare is not None:
            prepare(self)

        result = self._trigger(**overrides)

        assert result.triggered is False
        assert result.skipped_reason == expected_reason
        self.delay.assert_not_called()
        self.redis.set.assert_not_called()

    def test_a_slot_from_other_thresholds_does_not_stop_a_new_scan(self) -> None:
        # The ratios decide whether a finding exists, so a payload change has to re-analyze
        # instead of leaving the old findings in place until the slot expires.
        _store_a_slot(self, thresholds="0.9:0.5")

        result = self._trigger()

        assert result.triggered is True
        assert self.delay.call_count == 1

    @parameterized.expand(
        [
            ("hogql", HogQLQuery(query="select 1")),
            ("trends", TrendsQuery(series=[EventsNode(event="$pageview")])),
            ("funnels", FunnelsQuery(series=[EventsNode(event="a"), EventsNode(event="b")])),
        ]
    )
    def test_enqueues_and_writes_a_pending_slot(self, _name, query) -> None:
        result = self._trigger(query=query)

        assert result.triggered is True
        assert result.skipped_reason is None
        assert self.delay.call_count == 1
        enqueued = self.delay.call_args.kwargs
        assert enqueued["cache_key"] == "cache_key_1"
        assert enqueued["rows_read"] == 10
        assert enqueued["duration_ms"] == 2000
        assert enqueued["trigger"] == "fresh"
        # The job rebuilds the query from this payload, so it has to validate back unchanged.
        assert type(query).model_validate(enqueued["query"]) == query

        key, payload = self.redis.set.call_args.args
        assert key == "query_scan:1:cache_key_1"
        assert json.loads(payload)["status"] == "pending"
        assert self.redis.set.call_args.kwargs["ex"] == 600
