from uuid import uuid4

from products.subscriptions.backend.facade import artifacts as artifacts_facade
from products.subscriptions.backend.temporal import artifacts


def test_artifact_activity_passes_only_the_team_and_completed_run_identifiers(monkeypatch) -> None:
    run_id = uuid4()
    calls: list[tuple[int, object]] = []

    def prepare(*, team_id: int, run_id: object) -> None:
        calls.append((team_id, run_id))

    monkeypatch.setattr(artifacts_facade, "prepare_proactive_artifact_for_run", prepare)

    artifacts.prepare_proactive_artifact(artifacts.ProactiveArtifactPreparationInput(team_id=17, run_id=run_id))

    assert calls == [(17, run_id)]
    assert artifacts.WORKFLOWS == [artifacts.PrepareProactiveArtifactWorkflow]
    assert artifacts.ACTIVITIES == [artifacts.prepare_proactive_artifact]
