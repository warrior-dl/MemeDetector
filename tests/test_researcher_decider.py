from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

import pytest

from meme_detector.pipeline_models import MinerBundle
from meme_detector.researcher.decider import decide_bundle


def _build_bundle() -> MinerBundle:
    return MinerBundle.model_validate(
        {
            "bundle_id": "bundle_decider_1",
            "insight": {
                "insight_id": "insight_decider_1",
                "bvid": "BV1DECIDE1",
                "collected_date": date(2026, 4, 9),
                "comment_text": "闭嘴，如果你惹怒了我，并且不讲异国日记！",
                "worth_investigating": True,
                "signal_score": 0.88,
                "reason": "模板句与实体混合。",
                "status": "bundled",
            },
            "video_refs": [],
            "spans": [
                {
                    "span_id": "span_decider_1",
                    "insight_id": "insight_decider_1",
                    "raw_text": "闭嘴，如果你惹怒了我",
                    "normalized_text": "闭嘴如果你惹怒了我",
                    "span_type": "template_core",
                    "char_start": 0,
                    "char_end": 10,
                    "confidence": 0.9,
                    "is_primary": True,
                    "query_priority": "high",
                    "reason": "模板句核心。",
                }
            ],
            "hypotheses": [
                {
                    "hypothesis_id": "hyp_decider_1",
                    "insight_id": "insight_decider_1",
                    "candidate_title": "闭嘴，如果你惹怒了我……",
                    "hypothesis_type": "template_meme",
                    "miner_opinion": "模板句才是传播核心。",
                    "support_score": 0.82,
                    "counter_score": 0.11,
                    "uncertainty_score": 0.18,
                    "suggested_action": "search_then_review",
                    "status": "queued",
                }
            ],
            "hypothesis_spans": [
                {
                    "hypothesis_id": "hyp_decider_1",
                    "span_id": "span_decider_1",
                    "role": "primary",
                }
            ],
            "evidences": [],
            "miner_summary": {
                "recommended_hypothesis_id": "hyp_decider_1",
                "should_queue_for_research": True,
                "reason": "模板句证据更强。",
            },
        }
    )


@pytest.mark.asyncio
async def test_decide_bundle_maps_llm_output_to_research_decision(monkeypatch):
    bundle = _build_bundle()

    monkeypatch.setattr(
        "meme_detector.researcher.decider.resolve_llm_config",
        lambda _name: SimpleNamespace(api_key="test-key", model="test-model"),
    )
    monkeypatch.setattr(
        "meme_detector.researcher.decider.build_async_openai_client",
        lambda *_args, **_kwargs: object(),
    )

    async def fake_request_json_chat_completion(**_kwargs):
        return json.dumps(
            {
                "decision": "rewrite_title",
                "target_hypothesis_index": 0,
                "final_title": "闭嘴，如果你惹怒了我……",
                "target_record_id": "",
                "confidence": 0.86,
                "reason": "传播核心是模板句。",
                "evidence_summary": {
                    "support_count": 2,
                    "counter_count": 0,
                    "unclear_count": 0,
                },
                "assessment": {
                    "is_core_meme_unit": True,
                    "is_reusable_expression": True,
                    "is_entity_reference_only": False,
                    "needs_human_review": False,
                    "competing_hypothesis_exists": False,
                },
                "record": {
                    "alias": ["闭嘴，如果你惹怒了我"],
                    "definition": "一种模仿放狠话的模板句式。",
                    "origin": "常见于二创改写场景。",
                    "category": ["二次元", "其他"],
                    "platform": "Bilibili",
                    "heat_index": 66,
                    "lifecycle_stage": "emerging",
                    "source_urls": ["https://example.com/source"],
                    "confidence_score": 0.86,
                },
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "meme_detector.researcher.decider.request_json_chat_completion",
        fake_request_json_chat_completion,
    )

    decision = await decide_bundle(bundle, today=date(2026, 4, 9))

    assert decision.bundle_id == "bundle_decider_1"
    assert decision.decision.value == "rewrite_title"
    assert decision.target_hypothesis_id == "hyp_decider_1"
    assert decision.final_title == "闭嘴，如果你惹怒了我……"
    assert decision.record is not None
    assert decision.record.id == "闭嘴，如果你惹怒了我……"
    assert decision.record.title == "闭嘴，如果你惹怒了我……"


@pytest.mark.asyncio
async def test_decide_bundle_requires_configured_api_key(monkeypatch):
    monkeypatch.setattr(
        "meme_detector.researcher.decider.resolve_llm_config",
        lambda _name: SimpleNamespace(api_key="   ", model="test-model"),
    )

    with pytest.raises(RuntimeError, match="未配置"):
        await decide_bundle(_build_bundle(), today=date(2026, 4, 9))


@pytest.mark.asyncio
async def test_decide_bundle_normalizes_record_fields_from_llm(monkeypatch):
    bundle = _build_bundle()

    monkeypatch.setattr(
        "meme_detector.researcher.decider.resolve_llm_config",
        lambda _name: SimpleNamespace(api_key="test-key", model="test-model"),
    )
    monkeypatch.setattr(
        "meme_detector.researcher.decider.build_async_openai_client",
        lambda *_args, **_kwargs: object(),
    )

    async def fake_request_json_chat_completion(**_kwargs):
        return json.dumps(
            {
                "decision": "accept",
                "target_hypothesis_index": 0,
                "final_title": "闭嘴，如果你惹怒了我……",
                "target_record_id": "",
                "confidence": 0.91,
                "reason": "记录字段有轻微格式漂移，但语义明确。",
                "evidence_summary": {
                    "support_count": 3,
                    "counter_count": 0,
                    "unclear_count": 0,
                },
                "assessment": {
                    "is_core_meme_unit": True,
                    "is_reusable_expression": True,
                    "is_entity_reference_only": False,
                    "needs_human_review": False,
                    "competing_hypothesis_exists": False,
                },
                "record": {
                    "alias": "闭嘴，如果你惹怒了我",
                    "definition": "一种模仿放狠话的模板句式。",
                    "origin": "常见于二创改写场景。",
                    "category": "谐音梗/鬼畜梗",
                    "platform": "Bilibili",
                    "heat_index": 66,
                    "lifecycle_stage": "增长期",
                    "source_urls": "https://example.com/source",
                    "confidence_score": 0.86,
                },
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(
        "meme_detector.researcher.decider.request_json_chat_completion",
        fake_request_json_chat_completion,
    )

    decision = await decide_bundle(bundle, today=date(2026, 4, 9))

    assert decision.record is not None
    assert decision.record.alias == ["闭嘴，如果你惹怒了我"]
    assert decision.record.category == ["谐音", "其他"]
    assert decision.record.lifecycle_stage == "emerging"
    assert decision.record.source_urls == ["https://example.com/source"]
