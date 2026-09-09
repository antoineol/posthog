from posthog.test.base import APIBaseTest
from unittest import mock

from parameterized import parameterized

from products.data_warehouse.backend.max_tools import HogQLQueryFixerTool


class TestFixHogQL(APIBaseTest):
    def test_create(self):
        with (
            mock.patch("products.data_warehouse.backend.max_tools.MaxChatOpenAI"),
            mock.patch.object(HogQLQueryFixerTool, "_parse_output", return_value="select timestamp from events"),
        ):
            response = self.client.post(
                f"/api/environments/{self.team.id}/fix_hogql/",
                {"query": "select timestam from events", "error": "Unable to resolve field: timestam"},
            )

            assert response.status_code == 200

    @parameterized.expand(
        [
            ("without_connection", None, {"hogql_query": "q", "error_message": "e"}),
            (
                "with_connection",
                "018f0000-0000-0000-0000-000000000000",
                {
                    "hogql_query": "q",
                    "error_message": "e",
                    "connection_id": "018f0000-0000-0000-0000-000000000000",
                },
            ),
        ]
    )
    def test_context_passed_correctly(self, _name, connection_id, expected_context):
        query = "select timestam from events"
        error = "Unable to resolve field: timestam"
        expected_context = {**expected_context, "hogql_query": query, "error_message": error}

        captured_tool = None

        def capture_tool_init(original_init):
            def wrapper(self, *args, **kwargs):
                nonlocal captured_tool
                result = original_init(self, *args, **kwargs)
                captured_tool = self
                return result

            return wrapper

        body = {"query": query, "error": error}
        if connection_id is not None:
            body["connection_id"] = connection_id

        with (
            mock.patch("products.data_warehouse.backend.max_tools.MaxChatOpenAI"),
            mock.patch.object(HogQLQueryFixerTool, "_parse_output", return_value="select timestamp from events"),
            mock.patch.object(HogQLQueryFixerTool, "__init__", capture_tool_init(HogQLQueryFixerTool.__init__)),
        ):
            response = self.client.post(
                f"/api/environments/{self.team.id}/fix_hogql/",
                body,
            )

            assert response.status_code == 200
            assert captured_tool is not None
            assert captured_tool.context == expected_context

    def test_instruction_without_an_error_uses_the_advice_prompt(self):
        instruction = "Add an event filter naming the events this question is about. Change nothing else."

        with (
            mock.patch("products.data_warehouse.backend.max_tools.MaxChatOpenAI") as mock_model,
            mock.patch.object(
                HogQLQueryFixerTool, "_parse_output", return_value="select timestamp from events where event in ('…')"
            ),
        ):
            response = self.client.post(
                f"/api/environments/{self.team.id}/fix_hogql/",
                {"query": "select timestamp from events", "instruction": instruction},
            )

            assert response.status_code == 200
            messages = mock_model.return_value.with_structured_output.return_value.invoke.call_args[0][0]
            system_prompt, user_prompt = messages[0].content, messages[1].content

        assert "You apply one instruction to a HogQL query." in system_prompt
        assert "You fix HogQL errors" not in system_prompt
        assert instruction in user_prompt
        assert "<error>" not in user_prompt
