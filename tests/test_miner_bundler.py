from __future__ import annotations

from meme_detector.miner.bundler import _BundleSynthesis, _materialize_bundle


def _build_video() -> dict:
    return {
        "bvid": "BV1TESTBUNDLE",
        "collected_date": "2026-04-14",
        "partition": "鬼畜",
        "title": "测试视频",
        "description": "",
        "url": "https://www.bilibili.com/video/BV1TESTBUNDLE",
        "video_url": "https://www.bilibili.com/video/BV1TESTBUNDLE",
        "tags": ["测试"],
    }


def _build_insight() -> dict:
    return {
        "insight_id": "insight_test_bundle",
        "bvid": "BV1TESTBUNDLE",
        "collected_date": "2026-04-14",
        "comment_text": "厉害啊，人脸都很准。但好像没有大象叫[吃瓜]",
        "confidence": 0.82,
        "reason": "存在圈层音效线索。",
    }


def test_materialize_bundle_skips_empty_evidence_query():
    synthesis = _BundleSynthesis.model_validate(
        {
            "spans": [
                {
                    "text": "大象叫",
                    "span_type": "context_term",
                    "confidence": 0.88,
                    "is_primary": True,
                    "query_priority": "high",
                    "reason": "关键音效线索",
                }
            ],
            "hypotheses": [
                {
                    "title": "大象叫是鬼畜圈层音效梗",
                    "hypothesis_type": "entity_is_meme",
                    "miner_opinion": "更像圈层音效代称",
                    "support_score": 0.8,
                    "counter_score": 0.1,
                    "uncertainty_score": 0.2,
                    "suggested_action": "search_then_review",
                }
            ],
            "hypothesis_spans": [
                {
                    "hypothesis_index": 0,
                    "span_index": 0,
                    "role": "primary",
                }
            ],
            "evidences": [
                {
                    "hypothesis_index": 0,
                    "span_index": 0,
                    "query": "",
                    "query_mode": "contextual",
                    "source_kind": "web_search_summary",
                    "source_title": "空查询证据",
                    "source_url": "",
                    "snippet": "这条证据缺少 query",
                    "evidence_direction": "supports_origin",
                    "evidence_strength": 0.6,
                },
                {
                    "hypothesis_index": 0,
                    "span_index": 0,
                    "query": "华强买瓜 大象叫",
                    "query_mode": "contextual",
                    "source_kind": "web_search_summary",
                    "source_title": "有效证据",
                    "source_url": "https://example.com/source",
                    "snippet": "说明大象叫在该圈层中的语义",
                    "evidence_direction": "supports_origin",
                    "evidence_strength": 0.77,
                },
            ],
            "recommended_hypothesis_index": 0,
            "should_queue_for_research": True,
            "reason": "保留有效证据，跳过空 query。",
        }
    )

    bundle = _materialize_bundle(_build_video(), _build_insight(), synthesis)

    assert len(bundle.evidences) == 1
    assert bundle.evidences[0].query == "华强买瓜 大象叫"
    assert bundle.evidences[0].source_title == "有效证据"
