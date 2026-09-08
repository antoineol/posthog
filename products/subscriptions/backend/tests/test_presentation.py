from __future__ import annotations

from posthog.test.base import APIBaseTest

from django.test import override_settings

from products.subscriptions.backend.facade.research import PublicResearchCitation, PublicResearchResult
from products.subscriptions.backend.presentation.views import _response_payload


def test_research_response_payload_serializes_slotted_citations() -> None:
    payload = _response_payload(
        PublicResearchResult(
            citations=(
                PublicResearchCitation(
                    id="web:example",
                    url="https://example.com",
                    title="Example",
                    excerpt="Useful evidence.",
                ),
            )
        )
    )

    assert payload == {
        "citations": [
            {
                "id": "web:example",
                "url": "https://example.com",
                "title": "Example",
                "excerpt": "Useful evidence.",
            }
        ],
        "degradation": None,
    }


class TestProactiveConfigurationOptionsView(APIBaseTest):
    @override_settings(
        PULSE_PROACTIVE_ENABLED=True,
        PULSE_PUBLIC_RESEARCH_ENABLED=False,
        PULSE_ARTIFACT_PREPARATION_ENABLED=False,
    )
    def test_returns_instance_availability_for_the_current_project(self) -> None:
        response = self.client.get(f"/api/projects/{self.team.pk}/subscriptions/proactive_options/")

        assert response.status_code == 200
        assert response.json() == {
            "proactive_available": True,
            "public_web_research_available": False,
            "draft_pr_available": False,
            "repositories": [],
        }
