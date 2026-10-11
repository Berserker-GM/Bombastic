"""Provider-agnostic JSON completion. Tests pass a fake with the same method."""

import json
import re
from typing import Any, Protocol

import httpx

from radar.config import Settings, get_settings

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
_ANTHROPIC_VERSION = "2023-06-01"
_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


class LLM(Protocol):
    def complete_json(self, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
        """Return one JSON object for prompt that matches schema."""


class LLMConfigError(Exception):
    """LLM_PROVIDER or LLM_MODEL is missing or unsupported."""


class InvalidJSONError(Exception):
    """The provider response was not a JSON object."""


class AnthropicLLM:
    def __init__(self, model: str, api_key: str, *, timeout: float = 60.0) -> None:
        self.model = model
        self._api_key = api_key
        self._timeout = timeout

    def __repr__(self) -> str:
        return f"AnthropicLLM(model={self.model!r})"

    def complete_json(self, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "max_tokens": 4096,
            "tools": [
                {
                    "name": "record_extraction",
                    "description": "Record the extracted notice fields.",
                    "input_schema": schema,
                }
            ],
            "tool_choice": {"type": "tool", "name": "record_extraction"},
            "messages": [{"role": "user", "content": prompt}],
        }
        payload = _post_json(
            ANTHROPIC_URL,
            body,
            headers={
                "x-api-key": self._api_key,
                "anthropic-version": _ANTHROPIC_VERSION,
            },
            timeout=self._timeout,
        )
        return anthropic_json_object(payload)


class OpenAILLM:
    def __init__(self, model: str, api_key: str, *, timeout: float = 60.0) -> None:
        self.model = model
        self._api_key = api_key
        self._timeout = timeout

    def __repr__(self) -> str:
        return f"OpenAILLM(model={self.model!r})"

    def complete_json(self, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
        schema_text = json.dumps(schema, ensure_ascii=False)
        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "Reply with a JSON object that matches the schema.",
                },
                {
                    "role": "user",
                    "content": (f"{prompt}\n\nJSON schema:\n{schema_text}"),
                },
            ],
            "response_format": {"type": "json_object"},
        }
        payload = _post_json(
            OPENAI_URL,
            body,
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=self._timeout,
        )
        return openai_json_object(payload)


def llm_from_env(settings: Settings | None = None) -> LLM:
    resolved = settings if settings is not None else get_settings()
    provider = (resolved.llm_provider or "").strip().lower()
    model = (resolved.llm_model or "").strip()
    if not provider or not model:
        raise LLMConfigError("LLM_PROVIDER and LLM_MODEL are required")
    if provider == "anthropic":
        api_key = (resolved.anthropic_api_key or "").strip()
        if not api_key:
            raise LLMConfigError("ANTHROPIC_API_KEY is required")
        return AnthropicLLM(model=model, api_key=api_key)
    if provider == "openai":
        api_key = (resolved.openai_api_key or "").strip()
        if not api_key:
            raise LLMConfigError("OPENAI_API_KEY is required")
        return OpenAILLM(model=model, api_key=api_key)
    raise LLMConfigError(f"unsupported LLM_PROVIDER: {provider}")


def anthropic_json_object(payload: dict[str, Any]) -> dict[str, Any]:
    content = payload.get("content")
    if not isinstance(content, list):
        raise InvalidJSONError("anthropic response has no content")
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_use":
            continue
        tool_input = block.get("input")
        if isinstance(tool_input, dict):
            return tool_input
        if isinstance(tool_input, str):
            return parse_json_object(tool_input)
    for block in content:
        if isinstance(block, dict) and isinstance(block.get("text"), str):
            return parse_json_object(block["text"])
    raise InvalidJSONError("anthropic response had no JSON object")


def openai_json_object(payload: dict[str, Any]) -> dict[str, Any]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise InvalidJSONError("openai response has no choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        raise InvalidJSONError("openai response has no message")
    return parse_json_object(_message_text(message.get("content")))


def parse_json_object(text: str) -> dict[str, Any]:
    cleaned = _FENCE.sub("", text.strip())
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise InvalidJSONError("LLM response was not valid JSON") from exc
    if not isinstance(value, dict):
        raise InvalidJSONError("LLM response JSON was not an object")
    return value


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict) and isinstance(part.get("text"), str):
                parts.append(part["text"])
        if parts:
            return "".join(parts)
    raise InvalidJSONError("openai message content was empty")


def _post_json(
    url: str,
    body: dict[str, Any],
    *,
    headers: dict[str, str],
    timeout: float,
) -> dict[str, Any]:
    request_headers = {"content-type": "application/json", **headers}
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.post(url, headers=request_headers, json=body)
        response.raise_for_status()
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise InvalidJSONError("LLM response was not JSON") from exc
    if not isinstance(payload, dict):
        raise InvalidJSONError("LLM response was not a JSON object")
    return payload
