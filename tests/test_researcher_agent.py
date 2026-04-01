from datetime import date

import pytest

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_scout_raw_videos,
    upsert_scout_raw_videos,
)
from meme_detector.researcher.agent import _deep_analyze, deep_agent, run_research
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


@pytest.mark.asyncio
async def test_run_research_bootstraps_candidates_from_scout_raw(tmp_path, monkeypatch):
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

    async def fake_extract_candidate_seeds(_videos: list[dict]) -> list[dict]:
        return [
            {
                "word": "依托答辩",
                "score": 91.0,
                "is_new_word": True,
                "sample_comments": "- 这也太依托答辩了",
                "explanation": "Research 预筛候选",
                "video_refs": [
                    {
                        "bvid": "BV1BOOT001",
                        "partition": "鬼畜",
                        "title": "依托答辩合集",
                        "description": "测试视频",
                        "url": "https://www.bilibili.com/video/BV1BOOT001",
                        "matched_comment_count": 2,
                        "matched_comments": ["这也太依托答辩了"],
                    }
                ],
            }
        ]

    async def fake_batch_screen(_candidates: list[dict]) -> list[QuickScreenResult]:
        return [
            QuickScreenResult(
                word="依托答辩",
                is_meme=True,
                confidence=0.92,
                candidate_category="抽象",
                reason="是明显的抽象谐音梗",
            )
        ]

    async def fake_deep_analyze(**_kwargs) -> MemeRecord:
        return MemeRecord(
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

    accepted_ids: list[str] = []

    async def fake_upsert_meme(record: MemeRecord) -> None:
        accepted_ids.append(record.id)

    monkeypatch.setattr(
        "meme_detector.researcher.agent._extract_candidate_seeds",
        fake_extract_candidate_seeds,
    )
    monkeypatch.setattr(
        "meme_detector.researcher.agent._batch_screen",
        fake_batch_screen,
    )
    monkeypatch.setattr(
        "meme_detector.researcher.agent._deep_analyze",
        fake_deep_analyze,
    )
    monkeypatch.setattr(
        "meme_detector.researcher.agent.upsert_meme",
        fake_upsert_meme,
    )

    result = await run_research()

    assert result["bootstrapped_count"] == 1
    assert result["accepted_count"] == 1
    assert accepted_ids == ["依托答辩"]

    conn = get_conn()
    assert get_pending_scout_raw_videos(conn) == []
    row = conn.execute(
        "SELECT status FROM candidates WHERE word = ?",
        ["依托答辩"],
    ).fetchone()
    conn.close()

    assert row == ("accepted",)
