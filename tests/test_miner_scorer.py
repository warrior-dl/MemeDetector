from __future__ import annotations

import pytest

from meme_detector.miner import scorer


@pytest.mark.asyncio
async def test_score_video_comments_returns_fallback_when_llm_fails(monkeypatch):
    async def fake_get_bilibili_video_context(_bvid: str) -> dict:
        return {
            "status": "ready",
            "summary": "视频摘要",
            "content_text": "内容提炼",
            "transcript_excerpt": "字幕摘录",
        }

    recorder: dict[str, object] = {}

    class FakeCompletions:
        async def create(self, **kwargs):
            recorder["request_kwargs"] = kwargs
            raise TimeoutError("timed out")

    class FakeChat:
        def __init__(self):
            self.completions = FakeCompletions()

    class FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            recorder["client_kwargs"] = kwargs
            self.chat = FakeChat()

    monkeypatch.setattr(
        "meme_detector.miner.scorer.get_bilibili_video_context",
        fake_get_bilibili_video_context,
    )
    monkeypatch.setattr("meme_detector.miner.scorer.AsyncOpenAI", FakeAsyncOpenAI)
    monkeypatch.setattr("meme_detector.miner.scorer.get_current_run_id", lambda: None)
    monkeypatch.setattr("meme_detector.miner.scorer.settings.miner_llm_timeout_seconds", 12.5)
    monkeypatch.setattr("meme_detector.miner.scorer.settings.miner_llm_max_retries", 1)

    video = {
        "bvid": "BV1TEST001",
        "collected_date": "2026-04-03",
        "partition": "鬼畜",
        "title": "测试视频",
        "description": "测试简介",
        "url": "https://www.bilibili.com/video/BV1TEST001",
        "tags": ["抽象"],
    }

    results = await scorer._score_video_comments(video, ["第一条评论", "第二条评论"])

    assert len(results) == 2
    assert recorder["client_kwargs"]["timeout"] == 12.5
    assert recorder["client_kwargs"]["max_retries"] == 1
    assert all(item["confidence"] == 0.0 for item in results)
    assert all(item["is_meme_candidate"] is False for item in results)
    assert all(item["is_insider_knowledge"] is False for item in results)
    assert all("模型请求失败: timed out" in item["reason"] for item in results)


@pytest.mark.asyncio
async def test_score_video_comments_parses_llm_json(monkeypatch):
    async def fake_get_bilibili_video_context(_bvid: str) -> dict:
        return {
            "status": "ready",
            "summary": "视频摘要",
            "content_text": "",
            "transcript_excerpt": "",
        }

    class FakeResponse:
        def __init__(self, content: str):
            self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})()]

    class FakeCompletions:
        async def create(self, **kwargs):
            return FakeResponse(
                '{"results":[{"index":0,"is_meme_candidate":true,'
                '"is_insider_knowledge":false,"confidence":0.8,'
                '"reason":"像潜在梗"}]}'
            )

    class FakeChat:
        def __init__(self):
            self.completions = FakeCompletions()

    class FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            self.chat = FakeChat()

    monkeypatch.setattr(
        "meme_detector.miner.scorer.get_bilibili_video_context",
        fake_get_bilibili_video_context,
    )
    monkeypatch.setattr("meme_detector.miner.scorer.AsyncOpenAI", FakeAsyncOpenAI)
    monkeypatch.setattr("meme_detector.miner.scorer.get_current_run_id", lambda: None)

    video = {
        "bvid": "BV1TEST002",
        "collected_date": "2026-04-03",
        "partition": "鬼畜",
        "title": "测试视频",
        "description": "测试简介",
        "url": "https://www.bilibili.com/video/BV1TEST002",
        "tags": [],
    }

    results = await scorer._score_video_comments(video, ["第一条评论"])

    assert len(results) == 1
    assert results[0]["confidence"] == 0.8
    assert results[0]["is_meme_candidate"] is True
    assert results[0]["reason"] == "像潜在梗"
