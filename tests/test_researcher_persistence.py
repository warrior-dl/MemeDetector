from datetime import date

import pytest

from meme_detector.archivist.duckdb_store import get_conn, upsert_scout_candidates
from meme_detector.researcher.models import MemeRecord
from meme_detector.researcher.persistence import accept_candidate


@pytest.mark.asyncio
async def test_accept_candidate_writes_mirror_and_marks_accepted(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "research.db"),
    )

    conn = get_conn()
    upsert_scout_candidates(
        conn,
        [
            {
                "word": "鸡爪流",
                "score": 88.0,
                "is_new_word": True,
                "sample_comments": "- 鸡爪流",
                "explanation": "测试候选",
                "video_refs": [],
            }
        ],
    )
    conn.close()

    async def fake_upsert_meme(record: MemeRecord) -> None:
        assert record.id == "鸡爪流"

    monkeypatch.setattr(
        "meme_detector.researcher.persistence.upsert_meme",
        fake_upsert_meme,
    )

    record = MemeRecord(
        id="鸡爪流",
        title="鸡爪流",
        alias=["鸡爪流派"],
        definition="测试定义",
        origin="测试来源",
        category=["其他"],
        heat_index=71,
        lifecycle_stage="emerging",
        first_detected_at=date(2026, 4, 7),
        source_urls=["https://example.com/source"],
        confidence_score=0.88,
        updated_at=date(2026, 4, 7),
    )

    await accept_candidate("鸡爪流", record)

    conn = get_conn()
    candidate_row = conn.execute(
        "SELECT status FROM candidates WHERE word = ?",
        ["鸡爪流"],
    ).fetchone()
    mirror_row = conn.execute(
        "SELECT id, title, source_urls, confidence FROM meme_records WHERE id = ?",
        ["鸡爪流"],
    ).fetchone()
    conn.close()

    assert candidate_row == ("accepted",)
    assert mirror_row is not None
    assert mirror_row[0] == "鸡爪流"
    assert mirror_row[1] == "鸡爪流"
    assert "https://example.com/source" in mirror_row[2]
    assert mirror_row[3] == pytest.approx(0.88)
