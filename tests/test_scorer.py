"""
DuckDB 词频存储与 Score 计算测试。
"""

import pytest
from datetime import date, timedelta

from meme_detector.archivist.duckdb_store import (
    get_conn,
    upsert_word_freq,
    compute_candidates,
    get_pending_candidates,
    update_candidate_status,
)


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
