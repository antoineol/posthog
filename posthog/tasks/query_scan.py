from typing import Any

from celery import shared_task

from posthog.celery_queues import CeleryQueue
from posthog.models.team.team import Team
from posthog.models.user import User
from posthog.query_scan.job import QueryScanJob, run_query_scan
from posthog.query_scan.slot import PENDING_TTL_SECONDS
from posthog.scoping_audit import skip_team_scope_audit


# The queue cache warming and lazy precompute use, so a burst of analyses cannot overwhelm
# ClickHouse. The job is advisory, so a lost run costs nothing and there is no retry.
#
# `expires` matches the pending slot's lifetime. A task outliving its claim would let the next
# slow run enqueue a second copy, and a backlog would collect one per query per claim lifetime.
@shared_task(ignore_result=True, queue=CeleryQueue.ANALYTICS_LIMITED.value, expires=PENDING_TTL_SECONDS)
@skip_team_scope_audit  # Team and User are not team-scoped models
def analyze_query_scan(
    team_id: int,
    cache_key: str,
    query: dict[str, Any],
    modifiers: dict[str, Any] | None,
    insight_id: int | None,
    dashboard_id: int | None,
    rows_read: int,
    duration_ms: int,
    trigger: str,
    user_id: int | None,
    killed: bool = False,
    error_type: str | None = None,
) -> None:
    team = Team.objects.select_related("organization").filter(pk=team_id).first()
    if team is None:
        return
    # The user decides which warehouse tables resolve, so the job rebuilds the same tree the
    # person's run did. A deleted user leaves the query resolving without one.
    user = User.objects.filter(pk=user_id).first() if user_id is not None else None
    run_query_scan(
        QueryScanJob(
            team=team,
            user=user,
            cache_key=cache_key,
            query=query,
            modifiers=modifiers,
            insight_id=insight_id,
            dashboard_id=dashboard_id,
            rows_read=rows_read,
            duration_ms=duration_ms,
            trigger=trigger,
            killed=killed,
            error_type=error_type,
        )
    )
