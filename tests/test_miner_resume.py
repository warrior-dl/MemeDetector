from __future__ import annotations

from datetime import date

import pytest

from meme_detector.archivist.duckdb_store import (
    get_comment_bundle,
    get_conn,
    get_pending_scout_raw_videos,
    mark_scout_raw_videos_mined,
    upsert_scout_raw_videos,
)
from meme_detector.pipeline_models import MinerBundle
from meme_detector.miner.scorer import run_miner


@pytest.mark.asyncio
async def test_run_miner_marks_failed_video_and_continues(tmp_path, monkeypatch):
    db_path = str(tmp_path / "miner-resume.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    target_date = date(2026, 4, 3)
    conn = get_conn()
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1DONE001",
                "partition": "鬼畜",
                "title": "第一个视频",
                "description": "desc1",
                "url": "https://www.bilibili.com/video/BV1DONE001",
                "tags": ["梗"],
                "comments": ["第一条评论"],
            },
            {
                "bvid": "BV1PEND002",
                "partition": "鬼畜",
                "title": "第二个视频",
                "description": "desc2",
                "url": "https://www.bilibili.com/video/BV1PEND002",
                "tags": ["梗"],
                "comments": ["第二条评论"],
            },
        ],
        target_date,
    )
    conn.close()

    async def fake_score_video_comments(video: dict, comments: list[str]) -> list[dict]:
        if video["bvid"] == "BV1PEND002":
            raise RuntimeError("llm failed")
        return [
            {
                "insight_id": "insight-1",
                "bvid": video["bvid"],
                "collected_date": video["collected_date"],
                "partition": video["partition"],
                "title": video["title"],
                "description": video["description"],
                "video_url": video["url"],
                "tags": video["tags"],
                "comment_text": comments[0],
                "confidence": 0.91,
                "is_meme_candidate": True,
                "is_insider_knowledge": False,
                "reason": "像梗",
                "video_context": {"status": "ready"},
            }
        ]

    monkeypatch.setattr(
        "meme_detector.miner.scorer._score_video_comments",
        fake_score_video_comments,
    )
    async def fake_build_bundles(_video: dict, _insights: list[dict]):
        return []

    monkeypatch.setattr(
        "meme_detector.miner.scorer._build_bundles",
        fake_build_bundles,
    )

    result = await run_miner(target_date=target_date)

    conn = get_conn()
    insight_count = conn.execute(
        "SELECT COUNT(*) FROM miner_comment_insights WHERE bvid = 'BV1DONE001'"
    ).fetchone()[0]
    first_video_status = conn.execute(
        """
        SELECT miner_status
        FROM scout_raw_videos
        WHERE bvid = 'BV1DONE001' AND collected_date = ?
        """,
        [target_date],
    ).fetchone()[0]
    second_video_status = conn.execute(
        """
        SELECT miner_status
        FROM scout_raw_videos
        WHERE bvid = 'BV1PEND002' AND collected_date = ?
        """,
        [target_date],
    ).fetchone()[0]
    remaining = get_pending_scout_raw_videos(conn)
    conn.close()

    assert insight_count == 1
    assert result.failed_video_count == 1
    assert first_video_status == "processed"
    assert second_video_status == "failed"
    assert [item["bvid"] for item in remaining] == ["BV1PEND002"]


def test_upsert_scout_raw_videos_keeps_processed_status_for_same_day_duplicate(tmp_path, monkeypatch):
    db_path = str(tmp_path / "scout-same-day-dedup.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    target_date = date(2026, 4, 3)
    conn = get_conn()
    payload = {
        "bvid": "BV1DUP001",
        "partition": "鬼畜",
        "title": "重复视频",
        "description": "desc",
        "url": "https://www.bilibili.com/video/BV1DUP001",
        "tags": ["抽象", "梗"],
        "comments": ["第一条评论", "第二条评论", "第一条评论"],
    }
    upsert_scout_raw_videos(conn, [payload], target_date)
    mark_scout_raw_videos_mined(conn, [{"bvid": "BV1DUP001", "collected_date": target_date}])

    upsert_scout_raw_videos(conn, [payload], target_date)

    row = conn.execute(
        """
        SELECT miner_status, comment_count
        FROM scout_raw_videos
        WHERE bvid = ? AND collected_date = ?
        """,
        ["BV1DUP001", target_date],
    ).fetchone()
    pending = get_pending_scout_raw_videos(conn)
    conn.close()

    assert row == ("processed", 2)
    assert pending == []


def test_upsert_scout_raw_videos_resets_status_when_same_day_content_changes(tmp_path, monkeypatch):
    db_path = str(tmp_path / "scout-same-day-update.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    target_date = date(2026, 4, 3)
    conn = get_conn()
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1CHG001",
                "partition": "鬼畜",
                "title": "重复视频",
                "description": "desc",
                "url": "https://www.bilibili.com/video/BV1CHG001",
                "tags": ["抽象"],
                "comments": ["第一条评论"],
            }
        ],
        target_date,
    )
    mark_scout_raw_videos_mined(conn, [{"bvid": "BV1CHG001", "collected_date": target_date}])

    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1CHG001",
                "partition": "鬼畜",
                "title": "重复视频",
                "description": "desc",
                "url": "https://www.bilibili.com/video/BV1CHG001",
                "tags": ["抽象"],
                "comments": ["第一条评论", "新增评论"],
            }
        ],
        target_date,
    )

    row = conn.execute(
        """
        SELECT miner_status, comment_count
        FROM scout_raw_videos
        WHERE bvid = ? AND collected_date = ?
        """,
        ["BV1CHG001", target_date],
    ).fetchone()
    pending = get_pending_scout_raw_videos(conn)
    conn.close()

    assert row == ("pending", 2)
    assert [item["bvid"] for item in pending] == ["BV1CHG001"]


def test_upsert_scout_raw_videos_skips_cross_day_duplicate_snapshot(tmp_path, monkeypatch):
    db_path = str(tmp_path / "scout-cross-day-dedup.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    day_one = date(2026, 4, 3)
    day_two = date(2026, 4, 4)
    payload = {
        "bvid": "BV1CROSS01",
        "partition": "鬼畜",
        "title": "重复视频",
        "description": "desc",
        "url": "https://www.bilibili.com/video/BV1CROSS01",
        "tags": ["抽象"],
        "comments": ["第一条评论", "第二条评论"],
    }

    conn = get_conn()
    upsert_scout_raw_videos(conn, [payload], day_one)
    mark_scout_raw_videos_mined(conn, [{"bvid": "BV1CROSS01", "collected_date": day_one}])
    upsert_scout_raw_videos(conn, [payload], day_two)

    rows = conn.execute(
        """
        SELECT collected_date, miner_status
        FROM scout_raw_videos
        WHERE bvid = ?
        ORDER BY collected_date ASC
        """,
        ["BV1CROSS01"],
    ).fetchall()
    pending = get_pending_scout_raw_videos(conn)
    conn.close()

    assert rows == [(day_one, "processed")]
    assert pending == []


@pytest.mark.asyncio
async def test_run_miner_persists_comment_bundle(tmp_path, monkeypatch):
    db_path = str(tmp_path / "miner-bundle.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    target_date = date(2026, 4, 3)
    conn = get_conn()
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1BUNDLE1",
                "partition": "动画",
                "title": "模板句测试",
                "description": "desc",
                "url": "https://www.bilibili.com/video/BV1BUNDLE1",
                "tags": ["新番"],
                "comments": ["闭嘴，如果你惹怒了我，并且不讲异国日记！"],
            }
        ],
        target_date,
    )
    conn.close()

    async def fake_score_video_comments(video: dict, comments: list[str]) -> list[dict]:
        return [
            {
                "insight_id": "insight-bundle-1",
                "bvid": video["bvid"],
                "collected_date": video["collected_date"],
                "partition": video["partition"],
                "title": video["title"],
                "description": video["description"],
                "video_url": video["url"],
                "tags": video["tags"],
                "comment_text": comments[0],
                "confidence": 0.91,
                "is_meme_candidate": True,
                "is_insider_knowledge": False,
                "reason": "像模板梗",
                "video_context": {"status": "ready", "summary": "测试摘要"},
            }
        ]

    async def fake_build_bundles(video: dict, insights: list[dict]) -> list[MinerBundle]:
        assert video["bvid"] == "BV1BUNDLE1"
        assert insights[0]["insight_id"] == "insight-bundle-1"
        return [
            MinerBundle.model_validate(
                {
                    "bundle_id": "bundle_test_1",
                    "insight": {
                        "insight_id": "insight-bundle-1",
                        "bvid": "BV1BUNDLE1",
                        "collected_date": target_date,
                        "comment_text": insights[0]["comment_text"],
                        "worth_investigating": True,
                        "signal_score": 0.91,
                        "reason": "模板句与实体填槽混合。",
                        "status": "bundled",
                    },
                    "video_refs": [
                        {
                            "bvid": "BV1BUNDLE1",
                            "title": "模板句测试",
                            "url": "https://www.bilibili.com/video/BV1BUNDLE1",
                            "partition": "动画",
                            "collected_date": target_date,
                        }
                    ],
                    "spans": [
                        {
                            "span_id": "span_a",
                            "insight_id": "insight-bundle-1",
                            "raw_text": "闭嘴，如果你惹怒了我",
                            "normalized_text": "闭嘴如果你惹怒了我",
                            "span_type": "template_core",
                            "char_start": 0,
                            "char_end": 10,
                            "confidence": 0.88,
                            "is_primary": True,
                            "query_priority": "high",
                            "reason": "模板句。",
                        }
                    ],
                    "hypotheses": [
                        {
                            "hypothesis_id": "hyp_a",
                            "insight_id": "insight-bundle-1",
                            "candidate_title": "闭嘴，如果你惹怒了我……",
                            "hypothesis_type": "template_meme",
                            "miner_opinion": "模板句是传播核心。",
                            "support_score": 0.8,
                            "counter_score": 0.1,
                            "uncertainty_score": 0.2,
                            "suggested_action": "search_then_review",
                            "status": "queued",
                        }
                    ],
                    "hypothesis_spans": [
                        {"hypothesis_id": "hyp_a", "span_id": "span_a", "role": "primary"}
                    ],
                    "evidences": [],
                    "miner_summary": {
                        "recommended_hypothesis_id": "hyp_a",
                        "should_queue_for_research": True,
                        "reason": "模板句证据更强。",
                    },
                }
            )
        ]

    monkeypatch.setattr("meme_detector.miner.scorer._score_video_comments", fake_score_video_comments)
    monkeypatch.setattr("meme_detector.miner.scorer._build_bundles", fake_build_bundles)

    result = await run_miner(target_date=target_date)

    conn = get_conn()
    bundle = get_comment_bundle(conn, bundle_id="bundle_test_1")
    miner_status = conn.execute(
        "SELECT miner_status FROM scout_raw_videos WHERE bvid = ? AND collected_date = ?",
        ["BV1BUNDLE1", target_date],
    ).fetchone()[0]
    conn.close()

    assert result.bundle_count == 1
    assert bundle is not None
    assert bundle.hypotheses[0].candidate_title == "闭嘴，如果你惹怒了我……"
    assert miner_status == "processed"


@pytest.mark.asyncio
async def test_run_miner_bundle_failure_does_not_block_insight_persistence(tmp_path, monkeypatch):
    db_path = str(tmp_path / "miner-bundle-failure.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    target_date = date(2026, 4, 3)
    conn = get_conn()
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1BUNDLE2",
                "partition": "动画",
                "title": "模板句测试",
                "description": "desc",
                "url": "https://www.bilibili.com/video/BV1BUNDLE2",
                "tags": ["新番"],
                "comments": ["闭嘴，如果你惹怒了我，并且不讲异国日记！"],
            }
        ],
        target_date,
    )
    conn.close()

    async def fake_score_video_comments(video: dict, comments: list[str]) -> list[dict]:
        return [
            {
                "insight_id": "insight-bundle-2",
                "bvid": video["bvid"],
                "collected_date": video["collected_date"],
                "partition": video["partition"],
                "title": video["title"],
                "description": video["description"],
                "video_url": video["url"],
                "tags": video["tags"],
                "comment_text": comments[0],
                "confidence": 0.91,
                "is_meme_candidate": True,
                "is_insider_knowledge": False,
                "reason": "像模板梗",
                "video_context": {"status": "ready", "summary": "测试摘要"},
            }
        ]

    async def fake_build_bundles(_video: dict, _insights: list[dict]):
        raise RuntimeError("bundle failed")

    monkeypatch.setattr("meme_detector.miner.scorer._score_video_comments", fake_score_video_comments)
    monkeypatch.setattr("meme_detector.miner.scorer._build_bundles", fake_build_bundles)

    result = await run_miner(target_date=target_date)

    conn = get_conn()
    insight_count = conn.execute(
        "SELECT COUNT(*) FROM miner_comment_insights WHERE bvid = 'BV1BUNDLE2'"
    ).fetchone()[0]
    bundle_count = conn.execute("SELECT COUNT(*) FROM comment_insights").fetchone()[0]
    miner_status = conn.execute(
        "SELECT miner_status FROM scout_raw_videos WHERE bvid = ? AND collected_date = ?",
        ["BV1BUNDLE2", target_date],
    ).fetchone()[0]
    conn.close()

    assert result.bundle_count == 0
    assert insight_count == 1
    assert bundle_count == 0
    assert miner_status == "processed"
