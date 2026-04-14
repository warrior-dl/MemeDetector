from datetime import date

from meme_detector.archivist.duckdb_store import (
    get_comment_bundle,
    get_comment_bundle_detail,
    get_conn,
    get_research_decision,
    upsert_comment_bundle,
    upsert_research_decision,
)
from meme_detector.pipeline_models import MinerBundle, ResearchDecision


def _build_bundle() -> MinerBundle:
    return MinerBundle.model_validate(
        {
            "bundle_id": "bundle_001",
            "insight": {
                "insight_id": "insight_001",
                "bvid": "BV1TEST123",
                "collected_date": date(2026, 4, 9),
                "comment_text": "闭嘴，如果你惹怒了我，并且不讲异国日记！",
                "worth_investigating": True,
                "signal_score": 0.8,
                "reason": "模板句与实体填槽混合。",
                "status": "bundled",
            },
            "video_refs": [
                {
                    "bvid": "BV1TEST123",
                    "title": "测试视频",
                    "url": "https://www.bilibili.com/video/BV1TEST123",
                    "partition": "动画",
                    "collected_date": date(2026, 4, 9),
                }
            ],
            "spans": [
                {
                    "span_id": "span_1",
                    "insight_id": "insight_001",
                    "raw_text": "闭嘴，如果你惹怒了我",
                    "normalized_text": "闭嘴如果你惹怒了我",
                    "span_type": "template_core",
                    "char_start": 0,
                    "char_end": 10,
                    "confidence": 0.86,
                    "is_primary": True,
                    "query_priority": "high",
                    "reason": "可复用模板句。",
                },
                {
                    "span_id": "span_2",
                    "insight_id": "insight_001",
                    "raw_text": "异国日记",
                    "normalized_text": "异国日记",
                    "span_type": "entity_work",
                    "char_start": 14,
                    "char_end": 18,
                    "confidence": 0.92,
                    "is_primary": False,
                    "query_priority": "medium",
                    "reason": "更像作品名。",
                },
            ],
            "hypotheses": [
                {
                    "hypothesis_id": "hyp_1",
                    "insight_id": "insight_001",
                    "candidate_title": "闭嘴，如果你惹怒了我……",
                    "hypothesis_type": "template_meme",
                    "miner_opinion": "模板句才是传播核心。",
                    "support_score": 0.77,
                    "counter_score": 0.12,
                    "uncertainty_score": 0.23,
                    "suggested_action": "search_then_review",
                    "status": "queued",
                }
            ],
            "hypothesis_spans": [
                {
                    "hypothesis_id": "hyp_1",
                    "span_id": "span_1",
                    "role": "primary",
                },
                {
                    "hypothesis_id": "hyp_1",
                    "span_id": "span_2",
                    "role": "slot_filler",
                },
            ],
            "evidences": [
                {
                    "evidence_id": "ev_1",
                    "hypothesis_id": "hyp_1",
                    "span_id": "span_1",
                    "query": "闭嘴 如果你惹怒了我",
                    "query_mode": "literal",
                    "source_kind": "web_search_summary",
                    "source_title": "测试来源",
                    "source_url": "https://example.com/source",
                    "snippet": "这类句式常作为放狠话模板。",
                    "evidence_direction": "supports_template",
                    "evidence_strength": 0.74,
                }
            ],
            "miner_summary": {
                "recommended_hypothesis_id": "hyp_1",
                "should_queue_for_research": True,
                "reason": "模板句证据更强。",
            },
        }
    )


def _build_decision() -> ResearchDecision:
    return ResearchDecision.model_validate(
        {
            "decision_id": "decision_001",
            "bundle_id": "bundle_001",
            "target_hypothesis_id": "hyp_1",
            "decision": "rewrite_title",
            "final_title": "闭嘴，如果你惹怒了我……",
            "target_record_id": "闭嘴，如果你惹怒了我……",
            "confidence": 0.84,
            "reason": "传播核心是模板句，不是作品名填槽。",
            "evidence_summary": {
                "support_count": 2,
                "counter_count": 1,
                "unclear_count": 0,
            },
            "assessment": {
                "is_core_meme_unit": True,
                "is_reusable_expression": True,
                "is_entity_reference_only": False,
                "needs_human_review": False,
                "competing_hypothesis_exists": True,
            },
            "record": {
                "id": "闭嘴，如果你惹怒了我……",
                "title": "闭嘴，如果你惹怒了我……",
                "alias": ["闭嘴，如果你惹怒了我"],
                "definition": "一种模仿放狠话的模板句式。",
                "origin": "常见于表情包和二创改写语境。",
                "category": ["二次元", "其他"],
                "platform": "Bilibili",
                "heat_index": 68,
                "lifecycle_stage": "emerging",
                "first_detected_at": date(2026, 4, 9),
                "source_urls": ["https://example.com/source"],
                "confidence_score": 0.84,
                "human_verified": False,
                "updated_at": date(2026, 4, 9),
            },
        }
    )


def test_comment_bundle_round_trip_and_research_decision(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "bundle.db"),
    )

    bundle = _build_bundle()
    conn = get_conn()
    upsert_comment_bundle(conn, bundle)

    stored_bundle = get_comment_bundle(conn, bundle_id="bundle_001")
    assert stored_bundle is not None
    assert stored_bundle.bundle_id == "bundle_001"
    assert stored_bundle.hypotheses[0].candidate_title == "闭嘴，如果你惹怒了我……"
    assert stored_bundle.evidences[0].query_mode.value == "literal"

    upsert_research_decision(conn, _build_decision(), persist_record=True)
    stored_decision = get_research_decision(conn, "decision_001")
    assert stored_decision is not None
    assert stored_decision.decision.value == "rewrite_title"
    assert stored_decision.record is not None
    assert stored_decision.record.title == "闭嘴，如果你惹怒了我……"

    hypothesis_row = conn.execute(
        "SELECT status FROM hypotheses WHERE hypothesis_id = ?",
        ["hyp_1"],
    ).fetchone()
    insight_row = conn.execute(
        "SELECT status FROM comment_insights WHERE bundle_id = ?",
        ["bundle_001"],
    ).fetchone()
    meme_row = conn.execute(
        "SELECT title FROM meme_records WHERE id = ?",
        ["闭嘴，如果你惹怒了我……"],
    ).fetchone()
    conn.close()

    assert hypothesis_row == ("accepted",)
    assert insight_row == ("researched",)
    assert meme_row == ("闭嘴，如果你惹怒了我……",)


def test_get_comment_bundle_reconstructs_missing_primary_span_link(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "bundle-legacy.db"),
    )

    conn = get_conn()
    upsert_comment_bundle(conn, _build_bundle())
    conn.execute("DELETE FROM hypothesis_spans WHERE hypothesis_id = ?", ["hyp_1"])

    stored_bundle = get_comment_bundle(conn, bundle_id="bundle_001")
    detail = get_comment_bundle_detail(conn, "bundle_001")
    conn.close()

    assert stored_bundle is not None
    primary_links = [
        item for item in stored_bundle.hypothesis_spans
        if item.hypothesis_id == "hyp_1" and item.role.value == "primary"
    ]
    assert len(primary_links) == 1
    assert primary_links[0].span_id == "span_1"
    assert detail is not None
    assert detail["bundle"]["bundle_id"] == "bundle_001"
