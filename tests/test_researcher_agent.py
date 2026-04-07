from datetime import date

import pytest
from openai import AsyncOpenAI
from pydantic_ai.providers.deepseek import DeepSeekProvider
from pydantic_ai.providers.moonshotai import MoonshotAIProvider
from pydantic_ai.providers.openai import OpenAIProvider

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_scout_raw_videos,
    upsert_scout_candidates,
    upsert_miner_comment_insights,
    upsert_scout_raw_videos,
)
from meme_detector.researcher.agent import (
    _build_research_provider,
    _deep_analyze,
    deep_agent,
    run_research,
)
from meme_detector.researcher.models import MemeRecord, QuickScreenResult


class _FakeAgentResult:
    def __init__(self, output: MemeRecord):
        self.output = output

    def all_messages_json(self) -> bytes:
        return b"[]"

    def all_messages(self) -> list:
        return []


def test_deep_agent_does_not_expose_bibi_tool():
    assert "bilibili_video_context" not in deep_agent._function_toolset.tools


def test_deep_agent_exposes_byte_search_tools():
    assert "web_search_summary" in deep_agent._function_toolset.tools
    assert "web_search" in deep_agent._function_toolset.tools


def test_build_research_provider_uses_moonshot_for_kimi_models():
    provider = _build_research_provider(
        client=AsyncOpenAI(api_key="test", base_url="https://api.moonshot.ai/v1"),
        model_name="kimi-k2.5",
        base_url="https://api.moonshot.ai/v1",
    )
    assert isinstance(provider, MoonshotAIProvider)


def test_build_research_provider_uses_deepseek_for_deepseek_models():
    provider = _build_research_provider(
        client=AsyncOpenAI(api_key="test", base_url="https://api.deepseek.com"),
        model_name="deepseek-chat",
        base_url="https://api.deepseek.com",
    )
    assert isinstance(provider, DeepSeekProvider)


def test_build_research_provider_falls_back_to_openai_provider():
    provider = _build_research_provider(
        client=AsyncOpenAI(api_key="test", base_url="https://example.com/v1"),
        model_name="custom-chat",
        base_url="https://example.com/v1",
    )
    assert isinstance(provider, OpenAIProvider)


@pytest.mark.asyncio
async def test_deep_analyze_injects_prefetched_video_context(monkeypatch):
    prompts: list[str] = []

    async def fake_get_bilibili_video_context(bvid: str) -> dict:
        return {
            "bvid": bvid,
            "video_url": f"https://www.bilibili.com/video/{bvid}",
            "title": "测试视频",
            "status": "ready",
            "summary": "这是预取的 Bibi 摘要",
            "transcript_excerpt": "第一句字幕 第二句字幕",
            "chapters": [{"timestamp": "00:00", "title": "开场"}],
        }

    async def fake_run(prompt: str):
        prompts.append(prompt)
        return _FakeAgentResult(
            MemeRecord(
                id="依托答辩",
                title="依托答辩",
                alias=[],
                definition="测试定义",
                origin="测试来源",
                category=["其他"],
                heat_index=60,
                lifecycle_stage="emerging",
                first_detected_at=date(2026, 3, 31),
                source_urls=[],
                confidence_score=0.9,
                updated_at=date(2026, 3, 31),
            )
        )

    monkeypatch.setattr(
        "meme_detector.researcher.agent.get_bilibili_video_context",
        fake_get_bilibili_video_context,
    )
    async def fake_web_search_summary(_query: str, num_results: int = 5) -> dict:
        assert num_results == 5
        return {
            "summary": (
                "依托答辩通常被用作抽象谐音梗，常见于B站评论区和鬼畜语境，"
                "既指代戏谑表达，也会被当作二创场景中的标签化用语。"
            ),
            "results": [
                {
                    "title": "依托答辩词条",
                    "link": "https://example.com/wiki",
                    "snippet": "依托答辩是抽象谐音梗。",
                    "content": (
                        "依托答辩相关讨论通常会追溯到B站抽象文化语境，"
                        "既带有戏谑意味，也会被用于二创标签、评论区互文和鬼畜传播。"
                        "这一表达在传播中逐渐脱离原始上下文，形成更稳定的圈层识别符号。"
                    ),
                }
            ],
        }

    async def fake_web_search(_query: str, num_results: int = 5) -> list[dict]:
        assert num_results == 5
        return [{"title": "不应触发", "link": "https://example.com/unused", "snippet": "unused"}]

    monkeypatch.setattr(
        "meme_detector.researcher.agent.web_search_summary",
        fake_web_search_summary,
    )
    monkeypatch.setattr(
        "meme_detector.researcher.agent.web_search",
        fake_web_search,
    )
    monkeypatch.setattr("meme_detector.researcher.agent.get_current_run_id", lambda: None)
    monkeypatch.setattr(deep_agent, "run", fake_run)

    record = await _deep_analyze(
        word="依托答辩",
        sample_comments="- 这也太依托答辩了",
        video_refs=[
            {
                "bvid": "BV1TEST123",
                "title": "测试视频",
                "partition": "鬼畜",
                "url": "https://www.bilibili.com/video/BV1TEST123",
                "matched_comment_count": 2,
                "matched_comments": ["这也太依托答辩了"],
            }
        ],
        score=999.0,
        today=date(2026, 3, 31),
    )

    assert record is not None
    assert prompts
    assert "BV1TEST123" in prompts[0]
    assert "这是预取的 Bibi 摘要" in prompts[0]
    assert "Scout 关联视频背景" in prompts[0]
    assert "外部搜索上下文" in prompts[0]
    assert "总结版搜索是否足够: 是" in prompts[0]
    assert "普通网页搜索补充" not in prompts[0]


@pytest.mark.asyncio
async def test_deep_analyze_falls_back_to_web_search_when_summary_insufficient(monkeypatch):
    prompts: list[str] = []
    calls = {"summary": 0, "web": 0}

    async def fake_get_bilibili_video_context(_bvid: str) -> dict:
        return {"status": "unavailable", "skip_reason": "missing_api_token"}

    async def fake_web_search_summary(_query: str, num_results: int = 5) -> dict:
        calls["summary"] += 1
        assert num_results == 5
        return {
            "summary": "",
            "results": [
                {
                    "title": "弱结果",
                    "link": "https://example.com/weak",
                    "snippet": "很短",
                    "content": "",
                }
            ],
        }

    async def fake_web_search(_query: str, num_results: int = 5) -> list[dict]:
        calls["web"] += 1
        assert num_results == 5
        return [
            {
                "title": "补充来源",
                "link": "https://example.com/full",
                "snippet": "这里有更完整的来源线索。",
            }
        ]

    async def fake_run(prompt: str):
        prompts.append(prompt)
        return _FakeAgentResult(
            MemeRecord(
                id="依托答辩",
                title="依托答辩",
                alias=[],
                definition="测试定义",
                origin="测试来源",
                category=["其他"],
                heat_index=60,
                lifecycle_stage="emerging",
                first_detected_at=date(2026, 3, 31),
                source_urls=[],
                confidence_score=0.9,
                updated_at=date(2026, 3, 31),
            )
        )

    monkeypatch.setattr(
        "meme_detector.researcher.agent.get_bilibili_video_context",
        fake_get_bilibili_video_context,
    )
    monkeypatch.setattr(
        "meme_detector.researcher.agent.web_search_summary",
        fake_web_search_summary,
    )
    monkeypatch.setattr(
        "meme_detector.researcher.agent.web_search",
        fake_web_search,
    )
    monkeypatch.setattr("meme_detector.researcher.agent.get_current_run_id", lambda: None)
    monkeypatch.setattr(deep_agent, "run", fake_run)

    record = await _deep_analyze(
        word="依托答辩",
        sample_comments="- 这也太依托答辩了",
        video_refs=[],
        score=999.0,
        today=date(2026, 3, 31),
    )

    assert record is not None
    assert calls == {"summary": 1, "web": 1}
    assert prompts
    assert "总结版搜索是否足够: 否" in prompts[0]
    assert "普通网页搜索补充" in prompts[0]
    assert "补充来源" in prompts[0]


@pytest.mark.asyncio
async def test_run_research_does_not_auto_run_miner(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "research.db"),
    )

    conn = get_conn()
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1BOOT001",
                "partition": "鬼畜",
                "title": "依托答辩合集",
                "description": "测试视频",
                "url": "https://www.bilibili.com/video/BV1BOOT001",
                "comments": ["这也太依托答辩了", "依托答辩名场面"],
            }
        ],
        date(2026, 3, 31),
    )
    conn.close()

    conn = get_conn()
    upsert_scout_candidates(
        conn,
        [
            {
                "word": "依托答辩",
                "score": 91.0,
                "is_new_word": True,
                "sample_comments": "- 这也太依托答辩了",
                "explanation": "Research 预筛候选",
                "video_refs": [],
            }
        ],
    )
    conn.close()

    async def fail_batch_screen(_candidates: list[dict]) -> list[QuickScreenResult]:
        raise AssertionError("research should not screen candidates while miner backlog exists")

    monkeypatch.setattr(
        "meme_detector.researcher.agent._batch_screen",
        fail_batch_screen,
    )

    result = await run_research()

    assert result["bootstrapped_count"] == 0
    assert result["accepted_count"] == 0
    assert result["pending_count"] == 0
    assert result["blocked_pending_video_count"] == 1

    conn = get_conn()
    pending_videos = get_pending_scout_raw_videos(conn)
    row = conn.execute(
        "SELECT word, status FROM candidates WHERE word = ?",
        ["依托答辩"],
    ).fetchone()
    conn.close()

    assert len(pending_videos) == 1
    assert pending_videos[0]["bvid"] == "BV1BOOT001"
    assert row == ("依托答辩", "pending")
