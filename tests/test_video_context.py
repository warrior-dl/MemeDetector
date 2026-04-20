import httpx
import pytest

from meme_detector.archivist.schema import get_conn
from meme_detector.miner.video_context import get_bilibili_video_context


@pytest.fixture
def video_context_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "video-context.db")
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        db_path,
    )
    monkeypatch.setattr(
        "meme_detector.miner.video_context.settings.bibigpt_max_duration_seconds",
        900,
    )
    yield db_path


@pytest.mark.asyncio
async def test_long_video_skips_bibigpt_and_caches(monkeypatch, video_context_db):
    calls = {"api": 0}

    async def fake_fetch_video_info(_bvid: str) -> dict:
        return {"title": "长视频", "desc": "描述", "duration": 1200}

    async def fake_fetch_bibigpt_summary(_url: str) -> dict:
        calls["api"] += 1
        return {}

    monkeypatch.setattr(
        "meme_detector.miner.video_context._fetch_bilibili_video_info",
        fake_fetch_video_info,
    )
    monkeypatch.setattr(
        "meme_detector.miner.video_context._fetch_bibigpt_summary",
        fake_fetch_bibigpt_summary,
    )

    result = await get_bilibili_video_context("BV123LONG")

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "duration_exceeded"
    assert calls["api"] == 0

    conn = get_conn()
    cached = conn.execute("SELECT status, skip_reason FROM video_context_cache WHERE bvid = 'BV123LONG'").fetchone()
    conn.close()
    assert cached == ("skipped", "duration_exceeded")


@pytest.mark.asyncio
async def test_bibigpt_result_cached_and_reused(monkeypatch, video_context_db):
    calls = {"api": 0}

    async def fake_fetch_video_info(_bvid: str) -> dict:
        return {"title": "短视频", "desc": "简介", "duration": 600}

    async def fake_fetch_bibigpt_summary(_url: str) -> dict:
        calls["api"] += 1
        return {
            "summary": "这是总结",
            "detail": {
                "title": "短视频标题",
                "duration": 600,
                "descriptionText": "视频介绍",
                "contentText": "完整内容提炼",
                "chapters": [
                    {"timestamp": "00:00", "title": "开头", "summary": "背景介绍"},
                ],
                "subtitlesArray": [
                    {"text": "第一句字幕"},
                    {"text": "第二句字幕"},
                ],
            },
        }

    monkeypatch.setattr(
        "meme_detector.miner.video_context._fetch_bilibili_video_info",
        fake_fetch_video_info,
    )
    monkeypatch.setattr(
        "meme_detector.miner.video_context._fetch_bibigpt_summary",
        fake_fetch_bibigpt_summary,
    )
    monkeypatch.setattr(
        "meme_detector.miner.video_context.settings.bibigpt_api_token",
        "token",
    )

    first = await get_bilibili_video_context("BV123READY")
    second = await get_bilibili_video_context("BV123READY")

    assert first["status"] == "ready"
    assert first["summary"] == "这是总结"
    assert first["transcript_excerpt"] == "第一句字幕 第二句字幕"
    assert second["source"] == "cache"
    assert calls["api"] == 1


@pytest.mark.asyncio
async def test_bibigpt_timeout_returns_error_context_without_crashing(
    monkeypatch,
    video_context_db,
):
    async def fake_fetch_video_info(_bvid: str) -> dict:
        return {"title": "超时视频", "desc": "本地简介", "duration": 300}

    async def fake_fetch_bibigpt_summary(_url: str) -> dict:
        raise httpx.ReadTimeout("timed out")

    monkeypatch.setattr(
        "meme_detector.miner.video_context._fetch_bilibili_video_info",
        fake_fetch_video_info,
    )
    monkeypatch.setattr(
        "meme_detector.miner.video_context._fetch_bibigpt_summary",
        fake_fetch_bibigpt_summary,
    )
    monkeypatch.setattr(
        "meme_detector.miner.video_context.settings.bibigpt_api_token",
        "token",
    )

    result = await get_bilibili_video_context("BV123TIMEOUT")

    assert result["status"] == "error"
    assert result["skip_reason"] == "bibigpt_timeout"
    assert result["description_text"] == "本地简介"
    assert result["source"] == "local"
    assert result["error"] == "timed out"

    conn = get_conn()
    cached = conn.execute("SELECT COUNT(*) FROM video_context_cache WHERE bvid = 'BV123TIMEOUT'").fetchone()
    conn.close()
    assert cached == (0,)
