import json

import httpx
import pytest
import respx

from radar.config import Settings
from radar.llm.client import (
    ANTHROPIC_URL,
    OPENAI_URL,
    AnthropicLLM,
    InvalidJSONError,
    LLMConfigError,
    OpenAILLM,
    llm_from_env,
)


def test_llm_from_env_uses_provider_and_model() -> None:
    openai_llm = llm_from_env(
        Settings(
            llm_provider="OpenAI",
            llm_model="gpt-test",
            openai_api_key="sk-test",
        )
    )
    anthropic_llm = llm_from_env(
        Settings(
            llm_provider="anthropic",
            llm_model="claude-test",
            anthropic_api_key="sk-ant",
        )
    )

    assert isinstance(openai_llm, OpenAILLM)
    assert openai_llm.model == "gpt-test"
    assert isinstance(anthropic_llm, AnthropicLLM)
    assert anthropic_llm.model == "claude-test"


def test_llm_from_env_rejects_unknown_or_incomplete_settings() -> None:
    with pytest.raises(LLMConfigError):
        llm_from_env(Settings(llm_provider="google", llm_model="gemini"))
    with pytest.raises(LLMConfigError):
        llm_from_env(Settings(llm_provider="openai", llm_model=""))
    with pytest.raises(LLMConfigError):
        llm_from_env(
            Settings(
                llm_provider="anthropic",
                llm_model="claude-test",
                anthropic_api_key=None,
            )
        )


def test_openai_complete_json_returns_an_object() -> None:
    schema = {"type": "object", "properties": {"is_actionable": {"type": "boolean"}}}
    llm = OpenAILLM(model="gpt-test", api_key="test-key")

    with respx.mock:
        route = respx.post(OPENAI_URL).mock(
            return_value=httpx.Response(
                200,
                json={"choices": [{"message": {"content": '{"is_actionable": true}'}}]},
            )
        )
        result = llm.complete_json("extract this", schema)

    assert result == {"is_actionable": True}
    sent = json.loads(route.calls[0].request.content)
    assert sent["model"] == "gpt-test"
    assert sent["response_format"] == {"type": "json_object"}
    assert "extract this" in sent["messages"][1]["content"]
    assert '"is_actionable"' in sent["messages"][1]["content"]
    assert route.calls[0].request.headers["authorization"] == "Bearer test-key"


def test_anthropic_complete_json_reads_tool_input() -> None:
    schema = {"type": "object"}
    llm = AnthropicLLM(model="claude-test", api_key="test-key")

    with respx.mock:
        route = respx.post(ANTHROPIC_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "record_extraction",
                            "input": {"title": "Workshop"},
                        }
                    ]
                },
            )
        )
        result = llm.complete_json("extract this", schema)

    assert result == {"title": "Workshop"}
    sent = json.loads(route.calls[0].request.content)
    assert sent["model"] == "claude-test"
    assert sent["tools"][0]["input_schema"] == schema
    assert sent["tool_choice"] == {"type": "tool", "name": "record_extraction"}
    assert route.calls[0].request.headers["x-api-key"] == "test-key"


def test_openai_invalid_body_raises() -> None:
    llm = OpenAILLM(model="gpt-test", api_key="test-key")

    with respx.mock, pytest.raises(InvalidJSONError):
        respx.post(OPENAI_URL).mock(
            return_value=httpx.Response(
                200,
                json={"choices": [{"message": {"content": "not json"}}]},
            )
        )
        llm.complete_json("extract this", {"type": "object"})
