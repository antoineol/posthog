import json
from types import SimpleNamespace
from typing import Any

from unittest import mock

from django.test import SimpleTestCase

from parameterized import parameterized

from posthog.schema import EventsNode, FunnelsQuery, HogQLQuery, TrendsQuery

from posthog.hogql.query_stats import QueryStats

from posthog.clickhouse.query_tagging import AccessMethod, reset_query_tags, tag_queries
from posthog.models.user import User
from posthog.query_scan.flag import QueryScanFlag
from posthog.query_scan.trigger import maybe_trigger_query_scan
from posthog.shared_link_user import SharedLinkUser

FLAG = QueryScanFlag(mode="show", floor_ms=1000, event_ratio=0.1, persons_ratio=0.5)


def _shared_link_user() -> SharedLinkUser:
    # Carries no id the worker can resolve back, so the job would rebuild the query as no user.
    return SharedLinkUser(SimpleNamespace(enabled=True, team_id=1))  # type: ignore[arg-type]


def _tag_as_api_key(test: "TestQueryScanTrigger") -> None:
    tag_queries(access_method=AccessMethod.PERSONAL_API_KEY)


def _store_a_slot(test: "TestQueryScanTrigger", thresholds: str = FLAG.thresholds_fingerprint) -> None:
    test.redis.get.return_value = json.dumps({"version": 1, "status": "done", "findings": [], "thresholds": thresholds})


def _spend_the_enqueue_budget(test: "TestQueryScanTrigger") -> None:
    test.redis.incr.return_value = 11


def _lose_the_slot_claim(test: "TestQueryScanTrigger") -> None:
    # `SET NX` returns nothing when the key is already there, which is how a second run of the
    # same query learns that the first one claimed the slot.
    test.redis.set.return_value = None


class TestQueryScanTrigger(SimpleTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.redis = mock.Mock()
        self.redis.get.return_value = None
        self.redis.incr.return_value = 1
        patcher = mock.patch("posthog.query_scan.slot.query_cache_raw_client", return_value=self.redis)
        patcher.start()
        self.addCleanup(patcher.stop)
        delay_patcher = mock.patch("posthog.tasks.query_scan.analyze_query_scan.delay")
        self.delay = delay_patcher.start()
        self.addCleanup(delay_patcher.stop)
        self.addCleanup(reset_query_tags)

    def _assert_no_slot_was_claimed(self) -> None:
        # The enqueue counter writes through the same client, so look for the slot key itself.
        slot_writes = [call for call in self.redis.set.call_args_list if call.args[0] == "query_scan:1:cache_key_1"]
        assert slot_writes == []

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
            "user": User(id=7),
            "cacheable": True,
        }
        return maybe_trigger_query_scan(**{**arguments, **overrides})

    @parameterized.expand(
        [
            ("flag off", {"flag": None}, None, "flag_off"),
            ("below the floor", {"stats": QueryStats(rows_read=10, duration_ms=999.0)}, None, "below_floor"),
            ("api key run", {}, _tag_as_api_key, "api_key"),
            ("shared link viewer", {"user": _shared_link_user()}, None, "no_principal"),
            ("no principal at all", {"user": None}, None, "no_principal"),
            (
                "direct connection",
                {"query": HogQLQuery(query="select 1", connectionId="connection_1")},
                None,
                "direct_connection",
            ),
            ("result not cacheable", {"cacheable": False}, None, "not_cacheable"),
            ("slot already exists", {}, _store_a_slot, "slot_exists"),
            ("over the enqueues a team gets in a minute", {}, _spend_the_enqueue_budget, "rate_limited"),
        ]
    )
    def test_skips_with_a_reason(self, _name, overrides, prepare, expected_reason) -> None:
        if prepare is not None:
            prepare(self)

        result = self._trigger(**overrides)

        assert result.triggered is False
        assert result.skipped_reason == expected_reason
        self.delay.assert_not_called()
        self._assert_no_slot_was_claimed()

    def test_a_lost_slot_claim_does_not_enqueue_a_second_job(self) -> None:
        # Two slow runs of the same query can both find no slot, so the conditional write is what
        # keeps one job per slot; without it the loser would enqueue a duplicate analysis.
        _lose_the_slot_claim(self)

        result = self._trigger()

        assert result.triggered is False
        assert result.skipped_reason == "slot_exists"
        self.delay.assert_not_called()

    def test_a_broker_failure_does_not_fail_the_query(self) -> None:
        # ClickHouse has already done the work and the result is not cached yet, so an optional
        # side effect must not take a successful query down with it.
        self.delay.side_effect = Exception("broker unavailable")

        result = self._trigger()

        assert result.triggered is False
        assert result.skipped_reason == "enqueue_failed"
        self.redis.delete.assert_called_once_with("query_scan:1:cache_key_1")

    def test_a_killed_run_records_that_on_the_pending_slot(self) -> None:
        # The scan endpoint answers from this slot until the job finishes, so a stopped run that
        # left no `killed` here would be reported as one that ran to completion.
        self._trigger(killed=True)

        _key, payload = self.redis.set.call_args.args
        assert json.loads(payload)["killed"] is True

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
        assert enqueued["user_id"] == 7
        # The job rebuilds the query from this payload, so it has to validate back unchanged.
        assert type(query).model_validate(enqueued["query"]) == query

        key, payload = self.redis.set.call_args.args
        assert key == "query_scan:1:cache_key_1"
        assert json.loads(payload)["status"] == "pending"
        assert self.redis.set.call_args.kwargs["ex"] == 600
        assert self.redis.set.call_args.kwargs["nx"] is True
        # A count left without a TTL would stand forever and cap the team for good.
        self.redis.set.assert_any_call("query_scan:enqueues:1", 0, nx=True, ex=60)
