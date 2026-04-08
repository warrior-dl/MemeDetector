from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from openai import AsyncOpenAI
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.deepseek import DeepSeekProvider
from pydantic_ai.providers.moonshotai import MoonshotAIProvider
from pydantic_ai.providers.openai import OpenAIProvider

from meme_detector.config import settings

LLMTarget = Literal["default", "miner", "research"]
ProviderName = Literal["openai", "deepseek", "moonshotai"]


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
