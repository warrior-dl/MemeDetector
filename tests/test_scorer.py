"""
DuckDB 词频存储与 Score 计算测试。
"""

from datetime import date, timedelta

import pytest

from meme_detector.archivist.duckdb_store import (
    compute_candidates,
    get_conn,
    get_pending_candidates,
    get_pending_scout_raw_videos,
    update_candidate_status,
    upsert_word_freq,
)
from meme_detector.scout.collector import VideoTexts
from meme_detector.scout.scorer import _flatten_partition_videos, run_scout


@pytest.fixture
def conn(tmp_path, monkeypatch):
    """每个测试使用独立的临时数据库。"""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

    # 重新导入以使 monkeypatch 生效
    import importlib

    import meme_detector.archivist.duckdb_store as m
    importlib.reload(m)

    c = m.get_conn()
    yield c
    c.close()


TODAY = date(2026, 3, 24)
YESTERDAY = TODAY - timedelta(days=1)
TWO_WEEKS_AGO = TODAY - timedelta(days=14)


class TestUpsertWordFreq:
    def test_insert_and_query(self, conn):
        records = [{"word": "逆天", "freq": 10, "doc_count": 5}]
        upsert_word_freq(conn, records, "鬼畜", TODAY)

        row = conn.execute(
            "SELECT freq, doc_count FROM word_freq WHERE word='逆天' AND date=?",
            [TODAY]
        ).fetchone()
        assert row == (10, 5)

    def test_upsert_overwrites(self, conn):
        upsert_word_freq(conn, [{"word": "逆天", "freq": 10, "doc_count": 5}], "鬼畜", TODAY)
        upsert_word_freq(conn, [{"word": "逆天", "freq": 20, "doc_count": 8}], "鬼畜", TODAY)

        row = conn.execute(
            "SELECT freq FROM word_freq WHERE word='逆天' AND date=?",
            [TODAY]
        ).fetchone()
        assert row[0] == 20


class TestComputeCandidates:
    def _seed_baseline(self, conn, word: str, freq: int, days: int = 7):
        for i in range(1, days + 1):
            d = TODAY - timedelta(days=i)
            upsert_word_freq(conn, [{"word": word, "freq": freq, "doc_count": 2}], "鬼畜", d)

    def test_old_word_surge(self, conn):
        """老词词频暴增应进入候选。"""
        self._seed_baseline(conn, "逆天", freq=2)
        upsert_word_freq(conn, [{"word": "逆天", "freq": 30, "doc_count": 8}], "鬼畜", TODAY)

        candidates = compute_candidates(conn, TODAY, baseline_days=7, score_threshold=5.0)
        words = [c["word"] for c in candidates]
        assert "逆天" in words

    def test_old_word_normal(self, conn):
        """老词词频正常不应进入候选。"""
        self._seed_baseline(conn, "笑死", freq=10)
        upsert_word_freq(conn, [{"word": "笑死", "freq": 11, "doc_count": 5}], "鬼畜", TODAY)

        candidates = compute_candidates(conn, TODAY, baseline_days=7, score_threshold=5.0)
        words = [c["word"] for c in candidates]
        assert "笑死" not in words

    def test_new_word_with_enough_docs(self, conn):
        """新词出现在足够多视频中应进入候选。"""
        upsert_word_freq(conn, [{"word": "依托答辩", "freq": 15, "doc_count": 5}], "鬼畜", TODAY)

        candidates = compute_candidates(conn, TODAY, baseline_days=7, new_word_min_docs=3)
        words = [c["word"] for c in candidates]
        assert "依托答辩" in words

    def test_new_word_too_few_docs(self, conn):
        """新词出现视频数不足不应进入候选。"""
        upsert_word_freq(conn, [{"word": "依托答辩", "freq": 5, "doc_count": 1}], "鬼畜", TODAY)

        candidates = compute_candidates(conn, TODAY, baseline_days=7, new_word_min_docs=3)
        words = [c["word"] for c in candidates]
        assert "依托答辩" not in words

    def test_candidate_status_flow(self, conn):
        upsert_word_freq(conn, [{"word": "绷不住", "freq": 20, "doc_count": 5}], "鬼畜", TODAY)
        compute_candidates(conn, TODAY, baseline_days=7, new_word_min_docs=3)

        pending = get_pending_candidates(conn)
        assert any(c["word"] == "绷不住" for c in pending)

        update_candidate_status(conn, "绷不住", "accepted")
        pending_after = get_pending_candidates(conn)
        assert not any(c["word"] == "绷不住" for c in pending_after)


@pytest.mark.asyncio
async def test_run_scout_persists_raw_video_snapshots(tmp_path, monkeypatch):
    db_path = str(tmp_path / "scout.db")
    monkeypatch.setattr("meme_detector.archivist.duckdb_store.settings.duckdb_path", db_path)

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

    assert summary["video_count"] == 3
    assert summary["comment_count"] == 6

    conn = get_conn()
    raw_videos = get_pending_scout_raw_videos(conn)
    pending = get_pending_candidates(conn)
    conn.close()

    assert len(raw_videos) == 3
    target = next(item for item in raw_videos if item["bvid"] == "BV1TEST001")
    assert target["title"] == "依托答辩合集"
    assert target["tags"] == ["依托答辩", "抽象"]
    assert target["comments"] == ["这也太依托答辩了", "依托答辩名场面"]
    assert pending == []


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
