from __future__ import annotations

from datetime import date

import pytest

from meme_detector.miner import persistence as miner_persistence
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
    monkeypatch.setattr("meme_detector.llm_factory.resolve_async_openai_client_cls", lambda cls: cls)
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
    monkeypatch.setattr("meme_detector.llm_factory.resolve_async_openai_client_cls", lambda cls: cls)
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


def _make_pending_insight(
    *,
    insight_id: str = "insight-1",
    bvid: str = "BV1TESTBUNDLE",
    url: str | None = "https://www.bilibili.com/video/BV1TESTBUNDLE",
    partition: str = "",
    title: str = "",
    description: str = "",
    tags: list[str] | None = None,
) -> dict:
    """构造一条 _list_pending_bundle_insights 返回风格的字典。"""
    return {
        "insight_id": insight_id,
        "bvid": bvid,
        "collected_date": "2026-04-14",
        "partition": partition,
        "title": title,
        "description": description,
        "url": url,
        "tags": tags if tags is not None else [],
        "comment_text": "一条高价值评论",
        "confidence": 0.91,
        "is_meme_candidate": True,
        "is_insider_knowledge": False,
        "reason": "像潜在梗",
        "video_context": {"status": "ready"},
        "status": "pending_bundle",
    }


def test_list_pending_bundle_insights_wired_to_persistence():
    """Scorer 引用的 _list_pending_bundle_insights 必须来自 persistence 模块。"""
    assert scorer._list_pending_bundle_insights is miner_persistence.list_pending_bundle_insights


def test_list_pending_bundle_insights_returns_pending_rows_from_duckdb(tmp_path, monkeypatch):
    """整合 DuckDB 验证 _list_pending_bundle_insights 会按期望筛选并返回线索。"""
    from meme_detector.archivist.duckdb_store import (
        get_conn,
        mark_miner_comment_insights_processed,
        upsert_miner_comment_insights,
    )

    db_path = str(tmp_path / "list-pending-bundle.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    target_date = date(2026, 4, 3)
    conn = get_conn()
    upsert_miner_comment_insights(
        conn,
        [
            {
                "insight_id": "insight-pending-a",
                "bvid": "BV1PENDA",
                "collected_date": target_date,
                "partition": "鬼畜",
                "title": "视频 A",
                "description": "",
                "video_url": "https://www.bilibili.com/video/BV1PENDA",
                "tags": ["梗"],
                "comment_text": "高价值评论 A",
                "confidence": 0.75,
                "is_meme_candidate": True,
                "is_insider_knowledge": False,
                "reason": "A",
                "video_context": {"status": "ready"},
            },
            {
                "insight_id": "insight-pending-b",
                "bvid": "BV1PENDB",
                "collected_date": target_date,
                "partition": "鬼畜",
                "title": "视频 B",
                "description": "",
                "video_url": "https://www.bilibili.com/video/BV1PENDB",
                "tags": ["梗"],
                "comment_text": "高价值评论 B",
                "confidence": 0.95,
                "is_meme_candidate": True,
                "is_insider_knowledge": False,
                "reason": "B",
                "video_context": {"status": "ready"},
            },
            {
                "insight_id": "insight-done",
                "bvid": "BV1DONEX",
                "collected_date": target_date,
                "partition": "鬼畜",
                "title": "视频 X",
                "description": "",
                "video_url": "https://www.bilibili.com/video/BV1DONEX",
                "tags": ["梗"],
                "comment_text": "已结束的评论",
                "confidence": 0.99,
                "is_meme_candidate": True,
                "is_insider_knowledge": False,
                "reason": "done",
                "video_context": {"status": "ready"},
            },
        ],
    )
    # 把第三条推进到 bundled 状态，它就不应再出现在 pending 列表中。
    mark_miner_comment_insights_processed(conn, [{"insight_id": "insight-done"}])
    conn.close()

    pending = scorer._list_pending_bundle_insights()

    assert [item["insight_id"] for item in pending] == [
        "insight-pending-b",
        "insight-pending-a",
    ]
    assert all(item["status"] == "pending_bundle" for item in pending)
    # 确认返回字典在线索字段上与后续 run_miner_bundles 所依赖的结构一致。
    top = pending[0]
    assert top["bvid"] == "BV1PENDB"
    assert top["url"] == "https://www.bilibili.com/video/BV1PENDB"
    assert top["tags"] == ["梗"]
    assert top["video_context"] == {"status": "ready"}


@pytest.mark.asyncio
async def test_run_miner_bundles_returns_idle_result_when_no_pending_insights(monkeypatch):
    monkeypatch.setattr(scorer, "_list_pending_bundle_insights", lambda: [])

    def _unexpected(*_args, **_kwargs):  # pragma: no cover - 保护性断言
        raise AssertionError("应当未进入证据包处理循环")

    monkeypatch.setattr(scorer, "_mark_insight_bundling", _unexpected)
    monkeypatch.setattr(scorer, "_mark_insight_bundle_failed", _unexpected)
    monkeypatch.setattr(scorer, "_persist_comment_bundle", _unexpected)

    async def _unexpected_build(*_args, **_kwargs):  # pragma: no cover - 保护性断言
        raise AssertionError("应当未调用 _build_bundles")

    monkeypatch.setattr(scorer, "_build_bundles", _unexpected_build)

    result = await scorer.run_miner_bundles(target_date=date(2026, 4, 17))

    assert result.target_date == "2026-04-17"
    assert result.queued_insight_count == 0
    assert result.bundled_count == 0
    assert result.failed_insight_count == 0


@pytest.mark.asyncio
async def test_run_miner_bundles_bundles_each_pending_insight(monkeypatch):
    pending = [
        _make_pending_insight(insight_id="insight-a", bvid="BV1AAA"),
        _make_pending_insight(insight_id="insight-b", bvid="BV1BBB"),
    ]
    monkeypatch.setattr(scorer, "_list_pending_bundle_insights", lambda: list(pending))

    bundling_calls: list[dict] = []

    def _record_bundling(insight: dict) -> None:
        bundling_calls.append(insight)

    monkeypatch.setattr(scorer, "_mark_insight_bundling", _record_bundling)

    def _fail(_insight):  # pragma: no cover - 保护性断言
        raise AssertionError("本测试不应走到 bundle_failed 分支")

    monkeypatch.setattr(scorer, "_mark_insight_bundle_failed", _fail)

    persisted: list[object] = []
    monkeypatch.setattr(scorer, "_persist_comment_bundle", lambda bundle: persisted.append(bundle))

    build_calls: list[tuple[dict, list[dict]]] = []

    async def fake_build_bundles(video: dict, insights: list[dict]):
        build_calls.append((video, insights))
        return [f"bundle::{insights[0]['insight_id']}"]

    monkeypatch.setattr(scorer, "_build_bundles", fake_build_bundles)

    result = await scorer.run_miner_bundles(target_date=date(2026, 4, 17))

    assert result.queued_insight_count == 2
    assert result.bundled_count == 2
    assert result.failed_insight_count == 0
    assert [call["insight_id"] for call in bundling_calls] == ["insight-a", "insight-b"]
    # 每条 insight 应各自单独调用 _build_bundles，且只携带自身。
    assert len(build_calls) == 2
    assert [call[1][0]["insight_id"] for call in build_calls] == ["insight-a", "insight-b"]
    assert all(len(call[1]) == 1 for call in build_calls)
    assert persisted == ["bundle::insight-a", "bundle::insight-b"]


@pytest.mark.asyncio
async def test_run_miner_bundles_builds_video_payload_with_url_fallback(monkeypatch):
    """当 insight 没有 url 字段时，_build_bundles 收到的 video dict 应能回退到 video_url。"""
    insight = _make_pending_insight(
        insight_id="insight-fallback",
        bvid="BV1FALLBACK",
        url=None,
        partition="鬼畜",
        title="标题",
        description="简介",
        tags=["梗", "抽象"],
    )
    insight["video_url"] = "https://www.bilibili.com/video/BV1FALLBACK"

    monkeypatch.setattr(scorer, "_list_pending_bundle_insights", lambda: [insight])
    monkeypatch.setattr(scorer, "_mark_insight_bundling", lambda _i: None)
    monkeypatch.setattr(scorer, "_persist_comment_bundle", lambda _b: None)

    captured: dict = {}

    async def fake_build_bundles(video: dict, insights: list[dict]):
        captured["video"] = video
        captured["insights"] = insights
        return ["bundle"]

    monkeypatch.setattr(scorer, "_build_bundles", fake_build_bundles)

    await scorer.run_miner_bundles(target_date=date(2026, 4, 17))

    assert captured["video"] == {
        "bvid": "BV1FALLBACK",
        "collected_date": "2026-04-14",
        "partition": "鬼畜",
        "title": "标题",
        "description": "简介",
        "url": "https://www.bilibili.com/video/BV1FALLBACK",
        "video_url": "https://www.bilibili.com/video/BV1FALLBACK",
        "tags": ["梗", "抽象"],
    }
    assert captured["insights"] == [insight]


@pytest.mark.asyncio
async def test_run_miner_bundles_marks_bundle_failed_when_bundles_empty(monkeypatch):
    insight = _make_pending_insight(insight_id="insight-empty", bvid="BV1EMPTY")
    monkeypatch.setattr(scorer, "_list_pending_bundle_insights", lambda: [insight])
    monkeypatch.setattr(scorer, "_mark_insight_bundling", lambda _i: None)

    failed: list[dict] = []
    monkeypatch.setattr(scorer, "_mark_insight_bundle_failed", lambda i: failed.append(i))

    def _unexpected_persist(_bundle):  # pragma: no cover - 保护性断言
        raise AssertionError("空结果不应触发 _persist_comment_bundle")

    monkeypatch.setattr(scorer, "_persist_comment_bundle", _unexpected_persist)

    async def fake_build_bundles(_video: dict, _insights: list[dict]):
        return []

    monkeypatch.setattr(scorer, "_build_bundles", fake_build_bundles)

    result = await scorer.run_miner_bundles(target_date=date(2026, 4, 17))

    assert result.queued_insight_count == 1
    assert result.bundled_count == 0
    assert result.failed_insight_count == 1
    assert [item["insight_id"] for item in failed] == ["insight-empty"]


@pytest.mark.asyncio
async def test_run_miner_bundles_continues_after_build_exception(monkeypatch):
    """某条 insight 的 _build_bundles 抛错时，应标记失败并继续处理后续 insight。"""
    pending = [
        _make_pending_insight(insight_id="insight-bad", bvid="BV1BAD"),
        _make_pending_insight(insight_id="insight-good", bvid="BV1GOOD"),
    ]
    monkeypatch.setattr(scorer, "_list_pending_bundle_insights", lambda: list(pending))
    monkeypatch.setattr(scorer, "_mark_insight_bundling", lambda _i: None)

    failed: list[dict] = []
    monkeypatch.setattr(scorer, "_mark_insight_bundle_failed", lambda i: failed.append(i))

    persisted: list[object] = []
    monkeypatch.setattr(scorer, "_persist_comment_bundle", lambda b: persisted.append(b))

    async def fake_build_bundles(_video: dict, insights: list[dict]):
        if insights[0]["insight_id"] == "insight-bad":
            raise RuntimeError("模型请求失败")
        return [f"bundle::{insights[0]['insight_id']}"]

    monkeypatch.setattr(scorer, "_build_bundles", fake_build_bundles)

    result = await scorer.run_miner_bundles(target_date=date(2026, 4, 17))

    assert result.queued_insight_count == 2
    assert result.bundled_count == 1
    assert result.failed_insight_count == 1
    assert [item["insight_id"] for item in failed] == ["insight-bad"]
    assert persisted == ["bundle::insight-good"]


@pytest.mark.asyncio
async def test_run_miner_bundles_defaults_target_date_to_today(monkeypatch):
    """未传入 target_date 时应使用 date.today() 并回显在结果中。"""
    monkeypatch.setattr(scorer, "_list_pending_bundle_insights", lambda: [])

    class FakeDate(date):
        @classmethod
        def today(cls) -> date:
            return date(2026, 4, 17)

    monkeypatch.setattr(scorer, "date", FakeDate)

    result = await scorer.run_miner_bundles()

    assert result.target_date == "2026-04-17"
