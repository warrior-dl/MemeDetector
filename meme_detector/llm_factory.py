from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Literal

from openai import AsyncOpenAI, BadRequestError
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.deepseek import DeepSeekProvider
from pydantic_ai.providers.moonshotai import MoonshotAIProvider
from pydantic_ai.providers.openai import OpenAIProvider

from meme_detector.agent_tracing import resolve_async_openai_client_cls
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

LLMTarget = Literal["default", "miner", "research"]
ProviderName = Literal["openai", "deepseek", "moonshotai"]

logger = get_logger(__name__)

_JSON_PROMPT_ONLY_REMINDER = (
    "请只返回单个合法 JSON 对象或 JSON 数组，不要输出 Markdown 代码块、解释、前后缀文本或其他非 JSON 内容。"
)
_STRUCTURED_OUTPUT_SUPPORT_CACHE: dict[str, bool] = {}
_STRUCTURED_OUTPUT_PROBE_LOCKS: dict[str, asyncio.Lock] = {}


def _structured_output_probe_lock(model_name: str) -> asyncio.Lock:
    lock = _STRUCTURED_OUTPUT_PROBE_LOCKS.get(model_name)
    if lock is None:
        lock = asyncio.Lock()
        _STRUCTURED_OUTPUT_PROBE_LOCKS[model_name] = lock
    return lock


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
    effective_client_cls = resolve_async_openai_client_cls(client_cls)
    kwargs: dict = {
        "api_key": config.api_key,
        "base_url": config.base_url,
    }
    if timeout is not None:
        kwargs["timeout"] = timeout
    if max_retries is not None:
        kwargs["max_retries"] = max(max_retries, 0)
    return effective_client_cls(**kwargs)


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


async def _chat_completion_with_structured_output_fallback(
    *,
    client: AsyncOpenAI,
    model_name: str,
    messages: list[dict[str, str]],
) -> Any:
    """调用 chat.completions.create，必要时从 json_object 降级为 prompt-only。

    通过每模型一把 ``asyncio.Lock`` 序列化首次探测：并发调用进入时，只有
    拿到锁的协程对 ``response_format={"type": "json_object"}`` 发起真实请
    求；其余协程等待锁释放后读取 ``_STRUCTURED_OUTPUT_SUPPORT_CACHE`` 的
    结论，避免 N 条评论同时撞 400。
    """

    cached = _STRUCTURED_OUTPUT_SUPPORT_CACHE.get(model_name)
    if cached is None:
        async with _structured_output_probe_lock(model_name):
            cached = _STRUCTURED_OUTPUT_SUPPORT_CACHE.get(model_name)
            if cached is None:
                try:
                    resp = await client.chat.completions.create(
                        model=model_name,
                        messages=messages,
                        response_format={"type": "json_object"},
                    )
                    _STRUCTURED_OUTPUT_SUPPORT_CACHE[model_name] = True
                    return resp
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
                    cached = False

    if cached:
        try:
            resp = await client.chat.completions.create(
                model=model_name,
                messages=messages,
                response_format={"type": "json_object"},
            )
            return resp
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

    return await client.chat.completions.create(
        model=model_name,
        messages=build_prompt_only_json_messages(messages),
    )


async def request_json_chat_completion(
    *,
    client: AsyncOpenAI,
    model_name: str,
    messages: list[dict[str, str]],
) -> str:
    resp = await _chat_completion_with_structured_output_fallback(
        client=client,
        model_name=model_name,
        messages=messages,
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
    return "response_format.type" in lowered and "json_object" in lowered and "not supported" in lowered


def build_prompt_only_json_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    """确保消息列表里有一条 system 提示提醒模型只输出 JSON。

    语义（与重构前保持一致）：
    - 只处理**第一条** ``role == "system"`` 的消息：如果它的正文已经包含
      ``_JSON_PROMPT_ONLY_REMINDER`` 就原样返回，否则在末尾追加。
    - 如果整条消息链里**没有** system 消息，则在最前面插入一条只包含提醒的
      system 消息。
    - 所有原消息都会被浅拷贝，避免修改调用方传入的 dict。
    """

    patched = [dict(message) for message in messages]
    reminder = _JSON_PROMPT_ONLY_REMINDER

    first_system = next(
        (message for message in patched if message.get("role") == "system"),
        None,
    )
    if first_system is None:
        return [{"role": "system", "content": reminder}, *patched]

    content = str(first_system.get("content") or "").strip()
    if reminder not in content:
        first_system["content"] = f"{content}\n\n{reminder}" if content else reminder
    return patched


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


async def request_json_chat_completion_detailed(
    *,
    client: AsyncOpenAI,
    model_name: str,
    messages: list[dict[str, str]],
) -> dict[str, Any]:
    resp = await _chat_completion_with_structured_output_fallback(
        client=client,
        model_name=model_name,
        messages=messages,
    )
    content = resp.choices[0].message.content or "{}"
    usage = getattr(resp, "usage", None)
    usage_payload = {
        "prompt_tokens": getattr(usage, "prompt_tokens", 0) if usage else 0,
        "completion_tokens": getattr(usage, "completion_tokens", 0) if usage else 0,
        "total_tokens": getattr(usage, "total_tokens", 0) if usage else 0,
    }
    usage_payload["input_tokens"] = usage_payload["prompt_tokens"]
    usage_payload["output_tokens"] = usage_payload["completion_tokens"]
    return {
        "content": content,
        "usage": usage_payload,
    }


def _strip_markdown_code_fence(text: str) -> str:
    lines = text.strip().splitlines()
    if not lines:
        return text.strip()
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()
