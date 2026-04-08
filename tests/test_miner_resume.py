from __future__ import annotations

from datetime import date

import pytest

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_scout_raw_videos,
    mark_scout_raw_videos_mined,
    upsert_scout_raw_videos,
)
from meme_detector.miner.scorer import run_miner


@pytest.mark.asyncio
async def test_run_miner_persists_each_video_before_later_failure(tmp_path, monkeypatch):
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

    with pytest.raises(RuntimeError, match="llm failed"):
        await run_miner(target_date=target_date)

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
    assert first_video_status == "processed"
    assert second_video_status == "pending"
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
