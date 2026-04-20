import httpx
import pytest
from openai import BadRequestError

from meme_detector.config import settings
from meme_detector.llm_factory import (
    load_json_response,
    request_json_chat_completion,
    resolve_llm_config,
)


def test_resolve_llm_config_uses_global_defaults(monkeypatch):
    monkeypatch.setattr(settings, "llm_api_key", "global-key")
    monkeypatch.setattr(settings, "llm_base_url", "https://example.com/v1")
    monkeypatch.setattr(settings, "llm_model", "custom-model")
    monkeypatch.setattr(settings, "llm_provider", "openai")

    monkeypatch.setattr(settings, "miner_llm_api_key", "")
    monkeypatch.setattr(settings, "miner_llm_base_url", "")
    monkeypatch.setattr(settings, "miner_llm_model", "")
    monkeypatch.setattr(settings, "miner_llm_provider", "")

    config = resolve_llm_config("miner")

    assert config.api_key == "global-key"
    assert config.base_url == "https://example.com/v1"
    assert config.model == "custom-model"
    assert config.provider == "openai"


def test_resolve_llm_config_prefers_pipeline_overrides(monkeypatch):
    monkeypatch.setattr(settings, "llm_api_key", "global-key")
    monkeypatch.setattr(settings, "llm_base_url", "https://global.example.com/v1")
    monkeypatch.setattr(settings, "llm_model", "global-model")
    monkeypatch.setattr(settings, "llm_provider", "openai")

    monkeypatch.setattr(settings, "research_llm_api_key", "research-key")
    monkeypatch.setattr(settings, "research_llm_base_url", "https://api.moonshot.ai/v1")
    monkeypatch.setattr(settings, "research_llm_model", "kimi-k2.5")
    monkeypatch.setattr(settings, "research_llm_provider", "moonshotai")

    config = resolve_llm_config("research")

    assert config.api_key == "research-key"
    assert config.base_url == "https://api.moonshot.ai/v1"
    assert config.model == "kimi-k2.5"
    assert config.provider == "moonshotai"


def test_resolve_llm_config_auto_detects_provider(monkeypatch):
    monkeypatch.setattr(settings, "llm_api_key", "global-key")
    monkeypatch.setattr(settings, "llm_base_url", "https://api.deepseek.com")
    monkeypatch.setattr(settings, "llm_model", "deepseek-chat")
    monkeypatch.setattr(settings, "llm_provider", "auto")

    monkeypatch.setattr(settings, "research_llm_api_key", "")
    monkeypatch.setattr(settings, "research_llm_base_url", "")
    monkeypatch.setattr(settings, "research_llm_model", "")
    monkeypatch.setattr(settings, "research_llm_provider", "")

    config = resolve_llm_config("research")

    assert config.provider == "deepseek"


def test_load_json_response_supports_markdown_and_prefix_text():
    raw = """
    下面是结果：

    ```json
    {"results":[{"index":0,"reason":"ok"}]}
    ```
    """

    data = load_json_response(raw)

    assert data["results"][0]["index"] == 0


@pytest.mark.asyncio
async def test_request_json_chat_completion_falls_back_when_response_format_unsupported():
    calls: list[dict] = []

    class FakeResponse:
        def __init__(self, content: str):
            self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})()]

    class FakeCompletions:
        async def create(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise BadRequestError(
                    message="json_object not supported",
                    response=httpx.Response(
                        400,
                        request=httpx.Request("POST", "https://example.com/v1/chat/completions"),
                    ),
                    body={
                        "error": {
                            "param": "response_format.type",
                            "message": "json_object is not supported by this model",
                        }
                    },
                )
            return FakeResponse('{"ok": true}')

    class FakeChat:
        def __init__(self):
            self.completions = FakeCompletions()

    class FakeClient:
        def __init__(self):
            self.chat = FakeChat()

    raw = await request_json_chat_completion(
        client=FakeClient(),
        model_name="doubao-seed-2-0-pro-260215",
        messages=[
            {"role": "system", "content": "系统提示"},
            {"role": "user", "content": "用户提示"},
        ],
    )

    assert raw == '{"ok": true}'
    assert calls[0]["response_format"] == {"type": "json_object"}
    assert "response_format" not in calls[1]
    assert "请只返回单个合法 JSON 对象或 JSON 数组" in calls[1]["messages"][0]["content"]
