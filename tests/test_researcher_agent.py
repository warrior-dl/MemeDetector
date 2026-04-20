from datetime import date

import pytest

from meme_detector.archivist.miner_store import (
    get_comment_bundle,
    upsert_comment_bundle,
)
from meme_detector.archivist.schema import get_conn
from meme_detector.archivist.scout_store import get_pending_scout_raw_videos, upsert_scout_raw_videos
from meme_detector.pipeline_models import MinerBundle, ResearchDecision
from meme_detector.researcher.agent import run_research


@pytest.mark.asyncio
async def test_run_research_does_not_block_on_pending_miner_video(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(tmp_path / "research.db"),
    )

    conn = get_conn()
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1BOOT001",
                "partition": "鬼畜",
                "title": "依托答辩合集",
                "description": "测试视频",
                "url": "https://www.bilibili.com/video/BV1BOOT001",
                "comments": ["这也太依托答辩了", "依托答辩名场面"],
            }
        ],
        date(2026, 3, 31),
    )
    conn.close()

    result = await run_research()

    assert result.pending_count == 0
    assert result.accepted_count == 0
    assert result.rejected_count == 0
    assert result.blocked_pending_video_count == 0

    conn = get_conn()
    pending_videos = get_pending_scout_raw_videos(conn)
    conn.close()

    assert len(pending_videos) == 1
    assert pending_videos[0]["bvid"] == "BV1BOOT001"


@pytest.mark.asyncio
async def test_run_research_consumes_queued_bundle(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(tmp_path / "research-bundle.db"),
    )

    conn = get_conn()
    upsert_comment_bundle(
        conn,
        MinerBundle.model_validate(
            {
                "bundle_id": "bundle_research_1",
                "insight": {
                    "insight_id": "insight_research_1",
                    "bvid": "BV1RBUNDL1",
                    "collected_date": date(2026, 4, 9),
                    "comment_text": "闭嘴，如果你惹怒了我，并且不讲异国日记！",
                    "worth_investigating": True,
                    "signal_score": 0.91,
                    "reason": "模板句与实体填槽混合。",
                    "status": "bundled",
                },
                "video_refs": [
                    {
                        "bvid": "BV1RBUNDL1",
                        "title": "测试视频",
                        "url": "https://www.bilibili.com/video/BV1RBUNDL1",
                        "partition": "动画",
                        "collected_date": date(2026, 4, 9),
                    }
                ],
                "spans": [
                    {
                        "span_id": "span_research_1",
                        "insight_id": "insight_research_1",
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
                        "hypothesis_id": "hyp_research_1",
                        "insight_id": "insight_research_1",
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
                    {
                        "hypothesis_id": "hyp_research_1",
                        "span_id": "span_research_1",
                        "role": "primary",
                    }
                ],
                "evidences": [
                    {
                        "evidence_id": "ev_research_1",
                        "hypothesis_id": "hyp_research_1",
                        "span_id": "span_research_1",
                        "query": "闭嘴 如果你惹怒了我",
                        "query_mode": "literal",
                        "source_kind": "web_search_summary",
                        "source_title": "测试来源",
                        "source_url": "https://example.com/source",
                        "snippet": "这是模板句。",
                        "evidence_direction": "supports_template",
                        "evidence_strength": 0.8,
                    }
                ],
                "miner_summary": {
                    "recommended_hypothesis_id": "hyp_research_1",
                    "should_queue_for_research": True,
                    "reason": "模板句证据更强。",
                },
            }
        ),
    )
    conn.close()

    async def fake_decide_bundle(bundle, **_kwargs):
        assert bundle.bundle_id == "bundle_research_1"
        return ResearchDecision.model_validate(
            {
                "decision_id": "decision_bundle_research_1",
                "bundle_id": bundle.bundle_id,
                "target_hypothesis_id": bundle.hypotheses[0].hypothesis_id,
                "decision": "rewrite_title",
                "final_title": "闭嘴，如果你惹怒了我……",
                "target_record_id": "闭嘴，如果你惹怒了我……",
                "confidence": 0.86,
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
                    "competing_hypothesis_exists": False,
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
                    "confidence_score": 0.86,
                    "human_verified": False,
                    "updated_at": date(2026, 4, 9),
                },
            }
        )

    async def fake_verify_urls(urls: list[str]) -> list[str]:
        return urls

    monkeypatch.setattr("meme_detector.researcher.agent._decide_bundle", fake_decide_bundle)
    monkeypatch.setattr("meme_detector.researcher.agent.verify_urls", fake_verify_urls)

    result = await run_research()

    assert result.pending_count == 1
    assert result.adjudicated_count == 1
    assert result.accepted_count == 1
    assert result.rejected_count == 0

    conn = get_conn()
    bundle = get_comment_bundle(conn, bundle_id="bundle_research_1")
    decision_row = conn.execute(
        "SELECT decision FROM research_decisions WHERE decision_id = ?",
        ["decision_bundle_research_1"],
    ).fetchone()
    hypothesis_row = conn.execute(
        "SELECT status FROM hypotheses WHERE hypothesis_id = ?",
        ["hyp_research_1"],
    ).fetchone()
    meme_row = conn.execute(
        "SELECT title FROM meme_records WHERE id = ?",
        ["闭嘴，如果你惹怒了我……"],
    ).fetchone()
    conn.close()

    assert bundle is not None
    assert decision_row == ("rewrite_title",)
    assert hypothesis_row == ("accepted",)
    assert meme_row == ("闭嘴，如果你惹怒了我……",)


@pytest.mark.asyncio
async def test_run_research_consumes_evidenced_bundle(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(tmp_path / "research-evidenced-bundle.db"),
    )

    conn = get_conn()
    upsert_comment_bundle(
        conn,
        MinerBundle.model_validate(
            {
                "bundle_id": "bundle_research_evidenced_1",
                "insight": {
                    "insight_id": "insight_research_evidenced_1",
                    "bvid": "BV1REVID001",
                    "collected_date": date(2026, 4, 9),
                    "comment_text": "今天也要电子榨菜",
                    "worth_investigating": True,
                    "signal_score": 0.82,
                    "reason": "已有明确证据，但未进入深度排队。",
                    "status": "bundled",
                },
                "video_refs": [
                    {
                        "bvid": "BV1REVID001",
                        "title": "测试视频",
                        "url": "https://www.bilibili.com/video/BV1REVID001",
                        "partition": "动画",
                        "collected_date": date(2026, 4, 9),
                    }
                ],
                "spans": [
                    {
                        "span_id": "span_research_evidenced_1",
                        "insight_id": "insight_research_evidenced_1",
                        "raw_text": "电子榨菜",
                        "normalized_text": "电子榨菜",
                        "span_type": "quote_core",
                        "char_start": 5,
                        "char_end": 9,
                        "confidence": 0.9,
                        "is_primary": True,
                        "query_priority": "high",
                        "reason": "稳定复用短语。",
                    }
                ],
                "hypotheses": [
                    {
                        "hypothesis_id": "hyp_research_evidenced_1",
                        "insight_id": "insight_research_evidenced_1",
                        "candidate_title": "电子榨菜",
                        "hypothesis_type": "quote_meme",
                        "miner_opinion": "短语本身有独立传播性。",
                        "support_score": 0.78,
                        "counter_score": 0.08,
                        "uncertainty_score": 0.18,
                        "suggested_action": "search_optional",
                        "status": "evidenced",
                    }
                ],
                "hypothesis_spans": [
                    {
                        "hypothesis_id": "hyp_research_evidenced_1",
                        "span_id": "span_research_evidenced_1",
                        "role": "primary",
                    }
                ],
                "evidences": [
                    {
                        "evidence_id": "ev_research_evidenced_1",
                        "hypothesis_id": "hyp_research_evidenced_1",
                        "span_id": "span_research_evidenced_1",
                        "query": "电子榨菜",
                        "query_mode": "literal",
                        "source_kind": "web_search_result",
                        "source_title": "测试来源",
                        "source_url": "https://example.com/source",
                        "snippet": "常指下饭视频或陪伴式内容。",
                        "evidence_direction": "supports_meme",
                        "evidence_strength": 0.72,
                    }
                ],
                "miner_summary": {
                    "recommended_hypothesis_id": "hyp_research_evidenced_1",
                    "should_queue_for_research": False,
                    "reason": "现有证据已经足够进入轻量裁决。",
                },
            }
        ),
    )
    conn.close()

    async def fake_decide_bundle(bundle, **_kwargs):
        assert bundle.bundle_id == "bundle_research_evidenced_1"
        return ResearchDecision.model_validate(
            {
                "decision_id": "decision_bundle_research_evidenced_1",
                "bundle_id": bundle.bundle_id,
                "target_hypothesis_id": bundle.hypotheses[0].hypothesis_id,
                "decision": "accept",
                "final_title": "电子榨菜",
                "target_record_id": "电子榨菜",
                "confidence": 0.83,
                "reason": "已有证据足以支撑入库。",
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
                    "id": "电子榨菜",
                    "title": "电子榨菜",
                    "alias": [],
                    "definition": "指下饭视频或陪伴式内容的网络表达。",
                    "origin": "常见于视频消费场景中的口语表达。",
                    "category": ["其他"],
                    "platform": "Bilibili",
                    "heat_index": 64,
                    "lifecycle_stage": "emerging",
                    "first_detected_at": date(2026, 4, 9),
                    "source_urls": ["https://example.com/source"],
                    "confidence_score": 0.83,
                    "human_verified": False,
                    "updated_at": date(2026, 4, 9),
                },
            }
        )

    async def fake_verify_urls(urls: list[str]) -> list[str]:
        return urls

    monkeypatch.setattr("meme_detector.researcher.agent._decide_bundle", fake_decide_bundle)
    monkeypatch.setattr("meme_detector.researcher.agent.verify_urls", fake_verify_urls)

    result = await run_research()

    assert result.pending_count == 1
    assert result.adjudicated_count == 1
    assert result.accepted_count == 1


@pytest.mark.asyncio
async def test_run_research_processes_all_queued_bundles(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(tmp_path / "research-unlimited.db"),
    )

    def build_bundle(index: int) -> MinerBundle:
        bundle_id = f"bundle_research_unlimited_{index}"
        insight_id = f"insight_research_unlimited_{index}"
        span_id = f"span_research_unlimited_{index}"
        hypothesis_id = f"hyp_research_unlimited_{index}"
        title = f"测试标题{index}"
        return MinerBundle.model_validate(
            {
                "bundle_id": bundle_id,
                "insight": {
                    "insight_id": insight_id,
                    "bvid": f"BV1UNLIM{index:03d}",
                    "collected_date": date(2026, 4, 9),
                    "comment_text": f"测试评论{index}",
                    "worth_investigating": True,
                    "signal_score": 0.8,
                    "reason": "测试队列不限量读取。",
                    "status": "bundled",
                },
                "video_refs": [
                    {
                        "bvid": f"BV1UNLIM{index:03d}",
                        "title": f"测试视频{index}",
                        "url": f"https://www.bilibili.com/video/BV1UNLIM{index:03d}",
                        "partition": "动画",
                        "collected_date": date(2026, 4, 9),
                    }
                ],
                "spans": [
                    {
                        "span_id": span_id,
                        "insight_id": insight_id,
                        "raw_text": title,
                        "normalized_text": title,
                        "span_type": "template_core",
                        "char_start": 0,
                        "char_end": len(title),
                        "confidence": 0.88,
                        "is_primary": True,
                        "query_priority": "high",
                        "reason": "主传播片段。",
                    }
                ],
                "hypotheses": [
                    {
                        "hypothesis_id": hypothesis_id,
                        "insight_id": insight_id,
                        "candidate_title": title,
                        "hypothesis_type": "template_meme",
                        "miner_opinion": "测试假设。",
                        "support_score": 0.8,
                        "counter_score": 0.1,
                        "uncertainty_score": 0.2,
                        "suggested_action": "search_then_review",
                        "status": "queued",
                    }
                ],
                "hypothesis_spans": [
                    {
                        "hypothesis_id": hypothesis_id,
                        "span_id": span_id,
                        "role": "primary",
                    }
                ],
                "evidences": [],
                "miner_summary": {
                    "recommended_hypothesis_id": hypothesis_id,
                    "should_queue_for_research": True,
                    "reason": "测试证据包。",
                },
            }
        )

    conn = get_conn()
    upsert_comment_bundle(conn, build_bundle(1))
    upsert_comment_bundle(conn, build_bundle(2))
    conn.close()

    seen_bundle_ids: list[str] = []

    async def fake_decide_bundle(bundle, **_kwargs):
        seen_bundle_ids.append(bundle.bundle_id)
        return ResearchDecision.model_validate(
            {
                "decision_id": f"decision_{bundle.bundle_id}",
                "bundle_id": bundle.bundle_id,
                "target_hypothesis_id": bundle.hypotheses[0].hypothesis_id,
                "decision": "reject",
                "final_title": "",
                "target_record_id": "",
                "confidence": 0.7,
                "reason": "测试拒绝。",
                "evidence_summary": {
                    "support_count": 0,
                    "counter_count": 1,
                    "unclear_count": 0,
                },
                "assessment": {
                    "is_core_meme_unit": False,
                    "is_reusable_expression": False,
                    "is_entity_reference_only": True,
                    "needs_human_review": False,
                    "competing_hypothesis_exists": False,
                },
                "record": None,
            }
        )

    monkeypatch.setattr("meme_detector.researcher.agent._decide_bundle", fake_decide_bundle)

    result = await run_research()

    assert result.pending_count == 2
    assert result.adjudicated_count == 2
    assert result.rejected_count == 2
    assert seen_bundle_ids == [
        "bundle_research_unlimited_1",
        "bundle_research_unlimited_2",
    ]
