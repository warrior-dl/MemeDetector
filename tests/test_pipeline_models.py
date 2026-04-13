from datetime import date

import pytest
from pydantic import ValidationError

from meme_detector.pipeline_models import MinerBundle


def _build_bundle_payload() -> dict:
    return {
        "bundle_id": "bundle_001",
        "insight": {
            "insight_id": "insight_001",
            "bvid": "BV1TEST123",
            "collected_date": date(2026, 4, 9),
            "comment_text": "闭嘴，如果你惹怒了我，并且不讲异国日记！",
            "worth_investigating": True,
            "signal_score": 0.8,
            "reason": "模板句与实体填槽混合。",
            "status": "inspected",
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


def test_miner_bundle_requires_primary_span_link():
    payload = _build_bundle_payload()
    payload["hypothesis_spans"] = [
        {
            "hypothesis_id": "hyp_1",
            "span_id": "span_2",
            "role": "related",
        }
    ]

    with pytest.raises(ValidationError, match="missing primary span link"):
        MinerBundle.model_validate(payload)


def test_miner_bundle_validates_recommended_hypothesis():
    payload = _build_bundle_payload()
    payload["miner_summary"]["recommended_hypothesis_id"] = "hyp_missing"

    with pytest.raises(ValidationError, match="recommended_hypothesis_id"):
        MinerBundle.model_validate(payload)


def test_miner_bundle_accepts_valid_payload():
    bundle = MinerBundle.model_validate(_build_bundle_payload())

    assert bundle.bundle_id == "bundle_001"
    assert bundle.hypotheses[0].hypothesis_id == "hyp_1"
    assert bundle.miner_summary.should_queue_for_research is True
