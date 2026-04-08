from meme_detector.config import settings
from meme_detector.llm_factory import resolve_llm_config


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
