from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from openai import AsyncOpenAI, BadRequestError
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.deepseek import DeepSeekProvider
from pydantic_ai.providers.moonshotai import MoonshotAIProvider
from pydantic_ai.providers.openai import OpenAIProvider

from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

LLMTarget = Literal["default", "miner", "research"]
ProviderName = Literal["openai", "deepseek", "moonshotai"]

logger = get_logger(__name__)

_JSON_PROMPT_ONLY_REMINDER = (
    "请只返回单个合法 JSON 对象或 JSON 数组，不要输出 Markdown 代码块、解释、"
    "前后缀文本或其他非 JSON 内容。"
)
_STRUCTURED_OUTPUT_SUPPORT_CACHE: dict[str, bool] = {}


@dataclass(frozen=True)
class ResolvedLLMConfig:
    target: LLMTarget
    api_key: str
    base_url: str
    model: str
    provider: ProviderName


def resolve_llm_config(target: LLMTarget = "default") -> ResolvedLLMConfig:
    prefix = "" if target == "default" else f"{target}_"

    api_key = (_get_setting(f"{prefix}llm_api_key") or settings.llm_api_key).strip()
    base_url = (_get_setting(f"{prefix}llm_base_url") or settings.llm_base_url).strip()
    model = (_get_setting(f"{prefix}llm_model") or settings.llm_model).strip()
    provider_hint = (_get_setting(f"{prefix}llm_provider") or settings.llm_provider).strip()
    provider = normalize_provider_name(provider_hint, model_name=model, base_url=base_url)
    return ResolvedLLMConfig(
        target=target,
        api_key=api_key,
        base_url=base_url,
        model=model,
        provider=provider,
    )


def build_async_openai_client(
    target: LLMTarget = "default",
    *,
    timeout: float | None = None,
    max_retries: int | None = None,
    client_cls: type[AsyncOpenAI] = AsyncOpenAI,
) -> AsyncOpenAI:
    config = resolve_llm_config(target)
    kwargs: dict = {
        "api_key": config.api_key,
        "base_url": config.base_url,
    }
    if timeout is not None:
        kwargs["timeout"] = timeout
    if max_retries is not None:
        kwargs["max_retries"] = max(max_retries, 0)
    return client_cls(**kwargs)


def build_openai_chat_model(
    target: LLMTarget = "default",
    *,
    timeout: float | None = None,
    max_retries: int | None = None,
) -> OpenAIChatModel:
    config = resolve_llm_config(target)
    client = build_async_openai_client(
        target,
        timeout=timeout,
        max_retries=max_retries,
    )
    provider = build_provider(
        client=client,
        model_name=config.model,
        base_url=config.base_url,
        provider_hint=config.provider,
    )
    return OpenAIChatModel(
        config.model,
        provider=provider,
    )


def build_provider(
    *,
    client: AsyncOpenAI,
    model_name: str,
    base_url: str,
    provider_hint: str = "auto",
) -> OpenAIProvider | DeepSeekProvider | MoonshotAIProvider:
    provider = normalize_provider_name(provider_hint, model_name=model_name, base_url=base_url)
    if provider == "moonshotai":
        return MoonshotAIProvider(openai_client=client)
    if provider == "deepseek":
        return DeepSeekProvider(openai_client=client)
    return OpenAIProvider(openai_client=client)


def normalize_provider_name(
    provider_hint: str,
    *,
    model_name: str,
    base_url: str,
) -> ProviderName:
    normalized = provider_hint.strip().lower()
    if normalized in ("", "auto"):
        return infer_provider_name(model_name=model_name, base_url=base_url)
    if normalized in ("openai", "generic", "custom"):
        return "openai"
    if normalized in ("deepseek",):
        return "deepseek"
    if normalized in ("moonshot", "moonshotai", "kimi"):
        return "moonshotai"
    raise ValueError(f"Unsupported LLM provider: {provider_hint}")


def infer_provider_name(*, model_name: str, base_url: str) -> ProviderName:
    normalized_model = model_name.strip().lower()
    normalized_base_url = base_url.strip().lower()

    if normalized_model.startswith("kimi") or "moonshot" in normalized_base_url:
        return "moonshotai"
    if normalized_model.startswith("deepseek") or "deepseek" in normalized_base_url:
        return "deepseek"
    return "openai"


def _get_setting(name: str) -> str:
    value = getattr(settings, name, "")
    if isinstance(value, str):
        return value
    return ""


async def request_json_chat_completion(
    *,
    client: AsyncOpenAI,
    model_name: str,
    messages: list[dict[str, str]],
) -> str:
    use_structured_output = _STRUCTURED_OUTPUT_SUPPORT_CACHE.get(model_name, True)
    if use_structured_output:
        try:
            resp = await client.chat.completions.create(
                model=model_name,
                messages=messages,
                response_format={"type": "json_object"},
            )
            _STRUCTURED_OUTPUT_SUPPORT_CACHE[model_name] = True
            return resp.choices[0].message.content or "{}"
        except BadRequestError as exc:
            if not should_fallback_from_response_format(exc):
                raise
            _STRUCTURED_OUTPUT_SUPPORT_CACHE[model_name] = False
            logger.warning(
                "llm structured output unsupported, fallback to prompt-only json",
                extra={
                    "event": "llm_structured_output_fallback",
                    "model_name": model_name,
                },
            )

    resp = await client.chat.completions.create(
        model=model_name,
        messages=build_prompt_only_json_messages(messages),
    )
    return resp.choices[0].message.content or "{}"


def should_fallback_from_response_format(exc: BadRequestError) -> bool:
    error_payload = getattr(exc, "body", {}) or {}
    if isinstance(error_payload, dict):
        error_obj = error_payload.get("error", {})
        if isinstance(error_obj, dict):
            param = str(error_obj.get("param", "")).strip().lower()
            message = str(error_obj.get("message", "")).strip().lower()
            if param == "response_format.type" and "not supported" in message and "json_object" in message:
                return True

    lowered = str(exc).strip().lower()
    return (
        "response_format.type" in lowered
        and "json_object" in lowered
        and "not supported" in lowered
    )


def build_prompt_only_json_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    patched_messages = [dict(message) for message in messages]
    for message in patched_messages:
        if message.get("role") != "system":
            continue
        content = str(message.get("content", "")).strip()
        reminder = _JSON_PROMPT_ONLY_REMINDER
        if reminder not in content:
            message["content"] = f"{content}\n\n{reminder}" if content else reminder
        return patched_messages

    return [{"role": "system", "content": _JSON_PROMPT_ONLY_REMINDER}, *patched_messages]


def load_json_response(raw: str) -> Any:
    text = raw.strip()
    if not text:
        raise json.JSONDecodeError("Empty response", raw, 0)

    if text.startswith("```"):
        text = _strip_markdown_code_fence(text)

    decoder = json.JSONDecoder()
    candidate_positions = [index for index, ch in enumerate(text) if ch in "[{"]
    if text and text[0] in "[{":
        candidate_positions.insert(0, 0)

    seen_positions: set[int] = set()
    for start in candidate_positions:
        if start in seen_positions:
            continue
        seen_positions.add(start)
        try:
            value, _ = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        return value

    return json.loads(text)


def _strip_markdown_code_fence(text: str) -> str:
    lines = text.strip().splitlines()
    if not lines:
        return text.strip()
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()
