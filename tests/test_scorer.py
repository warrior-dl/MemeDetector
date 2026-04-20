"""
Scout 原始快照落库与视频去重测试。
"""

from datetime import date

import pytest

from meme_detector.archivist.schema import get_conn
from meme_detector.archivist.scout_store import get_pending_scout_raw_videos
from meme_detector.scout.collector import VideoTexts
from meme_detector.scout.scorer import _flatten_partition_videos, run_scout

TODAY = date(2026, 3, 24)


@pytest.mark.asyncio
async def test_run_scout_persists_raw_video_snapshots(tmp_path, monkeypatch):
    db_path = str(tmp_path / "scout.db")
    monkeypatch.setattr("meme_detector.archivist.schema.settings.duckdb_path", db_path)

    async def fake_collect_all_partitions():
        return {
            "鬼畜": [
                VideoTexts(
                    bvid="BV1TEST001",
                    partition="鬼畜",
                    title="依托答辩合集",
                    description="第一条视频",
                    url="https://www.bilibili.com/video/BV1TEST001",
                    comments=["这也太依托答辩了", "依托答辩名场面"],
                    tags=["依托答辩", "抽象"],
                ),
                VideoTexts(
                    bvid="BV1TEST002",
                    partition="鬼畜",
                    title="答辩现场",
                    description="第二条视频",
                    url="https://www.bilibili.com/video/BV1TEST002",
                    comments=["满屏都是依托答辩", "笑死"],
                    tags=["答辩"],
                ),
                VideoTexts(
                    bvid="BV1TEST003",
                    partition="鬼畜",
                    title="抽象时刻",
                    description="第三条视频",
                    url="https://www.bilibili.com/video/BV1TEST003",
                    comments=["这就是依托答辩", "太抽象了"],
                    tags=["抽象"],
                ),
            ]
        }

    monkeypatch.setattr(
        "meme_detector.scout.scorer.collect_all_partitions",
        fake_collect_all_partitions,
    )

    summary = await run_scout(target_date=TODAY)

    assert summary.video_count == 3
    assert summary.comment_count == 6

    conn = get_conn()
    raw_videos = get_pending_scout_raw_videos(conn)
    conn.close()

    assert len(raw_videos) == 3
    target = next(item for item in raw_videos if item["bvid"] == "BV1TEST001")
    assert target["title"] == "依托答辩合集"
    assert target["tags"] == ["依托答辩", "抽象"]
    assert target["comments"] == ["这也太依托答辩了", "依托答辩名场面"]
    assert target["miner_status"] == "pending"


def test_flatten_partition_videos_deduplicates_same_bvid_comments_and_snapshots():
    flattened, total_comments = _flatten_partition_videos(
        {
            "鬼畜": [
                VideoTexts(
                    bvid="BV1MERGE01",
                    partition="鬼畜",
                    title="重复视频",
                    description="desc",
                    url="https://www.bilibili.com/video/BV1MERGE01",
                    comments=["第一条评论", "第二条评论"],
                    tags=["抽象"],
                    comment_snapshots=[
                        {"rpid": 1001, "message": "第一条评论"},
                    ],
                ),
                VideoTexts(
                    bvid="BV1MERGE01",
                    partition="鬼畜",
                    title="重复视频",
                    description="desc",
                    url="https://www.bilibili.com/video/BV1MERGE01",
                    comments=["第二条评论", "第三条评论"],
                    tags=["鬼畜", "抽象"],
                    comment_snapshots=[
                        {"rpid": 1001, "message": "第一条评论"},
                        {"rpid": 1002, "message": "第三条评论"},
                    ],
                ),
            ]
        }
    )

    assert len(flattened) == 1
    assert total_comments == 3
    assert flattened[0]["comments"] == ["第一条评论", "第二条评论", "第三条评论"]
    assert flattened[0]["tags"] == ["抽象", "鬼畜"]
    assert [item["rpid"] for item in flattened[0]["comment_snapshots"]] == [1001, 1002]
