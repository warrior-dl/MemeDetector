from datetime import date

import pytest
from fastapi.testclient import TestClient

from meme_detector.archivist.agent_store import (
    create_agent_conversation,
    finish_agent_conversation,
)
from meme_detector.archivist.miner_store import upsert_comment_bundle, upsert_miner_comment_insights
from meme_detector.archivist.pipeline_run_store import create_pipeline_run, finish_pipeline_run
from meme_detector.archivist.research_store import upsert_research_decision
from meme_detector.archivist.schema import get_conn
from meme_detector.archivist.scout_store import upsert_scout_raw_videos


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.schema.settings.duckdb_path",
        str(tmp_path / "admin-test.db"),
    )
    monkeypatch.setattr(
        "meme_detector.archivist.scout_store.settings.media_asset_root",
        str(tmp_path / "assets"),
    )
    monkeypatch.setattr("meme_detector.api.app.ensure_index", lambda: None)

    def fake_download_media_asset(source_url: str) -> dict:
        asset_dir = tmp_path / "assets" / "comment-images" / "te"
        asset_dir.mkdir(parents=True, exist_ok=True)
        asset_path = asset_dir / "test-asset.png"
        asset_path.write_bytes(
            bytes.fromhex(
                "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de"
                "0000000c49444154789c6360000000020001e221bc330000000049454e44ae426082"
            )
        )
        return {
            "asset_id": "test-asset",
            "storage_path": str(asset_path),
            "sha256": "test-asset",
            "mime_type": "image/png",
            "file_ext": ".png",
            "width": 1,
            "height": 1,
            "byte_size": asset_path.stat().st_size,
            "download_status": "success",
            "last_error": "",
        }

    monkeypatch.setattr(
        "meme_detector.archivist.scout_store._download_media_asset",
        fake_download_media_asset,
    )

    async def fake_search_memes(*_args, **_kwargs):
        return {
            "estimatedTotalHits": 2,
            "hits": [
                {
                    "id": "meme-1",
                    "title": "依托答辩",
                    "category": ["抽象"],
                    "heat_index": 88,
                    "lifecycle_stage": "peak",
                }
            ],
        }

    monkeypatch.setattr("meme_detector.api.routes.search_memes", fake_search_memes)
    monkeypatch.setattr(
        "meme_detector.api.routes.get_scheduler_jobs",
        lambda: [
            {
                "id": "daily_scout",
                "name": "每日采集与原始入库",
                "next_run_time": "2026-03-28T02:05:00",
                "trigger": "cron[hour='2', minute='5']",
            },
            {
                "id": "daily_miner_insights",
                "name": "每日评论线索初筛",
                "next_run_time": "2026-03-28T03:00:00",
                "trigger": "cron[hour='3', minute='0']",
            },
            {
                "id": "daily_miner_bundles",
                "name": "每日证据包生成",
                "next_run_time": "2026-03-28T03:20:00",
                "trigger": "cron[hour='3', minute='20']",
            },
        ],
    )

    from meme_detector.api.app import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


def test_admin_page_and_runs_api(client):
    conn = get_conn()
    run_id = create_pipeline_run(conn, job_name="scout", trigger_mode="manual")
    miner_run_id = create_pipeline_run(conn, job_name="miner", trigger_mode="manual")
    research_run_id = create_pipeline_run(conn, job_name="research", trigger_mode="manual")
    upsert_scout_raw_videos(
        conn,
        [
            {
                "bvid": "BV1raw111",
                "partition": "动画",
                "title": "第一条 Scout 快照",
                "description": "原始描述 A",
                "url": "https://www.bilibili.com/video/BV1raw111",
                "tags": ["整活", "抽象", "二创"],
                "comments": ["第一条评论", "第二条评论"],
                "comment_snapshots": [
                    {
                        "rpid": 10001,
                        "root_rpid": 10001,
                        "parent_rpid": 10001,
                        "mid": 20001,
                        "uname": "测试用户",
                        "message": "第一条评论",
                        "like_count": 12,
                        "reply_count": 1,
                        "ctime": 1711600000,
                        "pictures": [
                            {
                                "img_src": "https://example.com/comment-1.png",
                                "img_width": 128,
                                "img_height": 128,
                                "img_size": 12.5,
                            }
                        ],
                        "content": {
                            "message": "第一条评论",
                            "pictures": [{"img_src": "https://example.com/comment-1.png"}],
                        },
                        "raw_reply": {"rpid": 10001},
                    },
                    {
                        "rpid": 10002,
                        "root_rpid": 10002,
                        "parent_rpid": 10002,
                        "mid": 20002,
                        "uname": "无图用户",
                        "message": "第二条评论",
                        "like_count": 3,
                        "reply_count": 0,
                        "ctime": 1711600010,
                        "pictures": [],
                        "content": {"message": "第二条评论"},
                        "raw_reply": {"rpid": 10002},
                    },
                ],
            },
            {
                "bvid": "BV1raw222",
                "partition": "鬼畜",
                "title": "第二条 Scout 快照",
                "description": "原始描述 B",
                "url": "https://www.bilibili.com/video/BV1raw222",
                "tags": ["鬼畜调音"],
                "comments": ["第三条评论"],
            },
        ],
        date(2026, 3, 28),
    )
    upsert_miner_comment_insights(
        conn,
        [
            {
                "insight_id": "miner-insight-1",
                "bvid": "BV1raw111",
                "collected_date": date(2026, 3, 28),
                "partition": "动画",
                "title": "第一条 Scout 快照",
                "description": "原始描述 A",
                "video_url": "https://www.bilibili.com/video/BV1raw111",
                "tags": ["整活", "抽象", "二创"],
                "comment_text": "这也太抽象了，全是圣经",
                "confidence": 0.91,
                "is_meme_candidate": True,
                "is_insider_knowledge": True,
                "reason": "评论包含稳定复用表达和圈层语境。",
                "video_context": {
                    "summary": "视频在讲抽象整活。",
                    "content_text": "完整视频内容摘要",
                    "transcript_excerpt": "这里开始整活",
                },
            }
        ],
    )
    upsert_comment_bundle(
        conn,
        {
            "bundle_id": "bundle-researched",
            "insight": {
                "insight_id": "miner-insight-1",
                "bvid": "BV1raw111",
                "collected_date": date(2026, 3, 28),
                "comment_text": "这也太抽象了，全是圣经",
                "worth_investigating": True,
                "signal_score": 0.91,
                "reason": "评论存在可复用表达与实体混合。",
                "status": "bundled",
            },
            "video_refs": [
                {
                    "bvid": "BV1raw111",
                    "title": "第一条 Scout 快照",
                    "url": "https://www.bilibili.com/video/BV1raw111",
                    "partition": "动画",
                    "collected_date": date(2026, 3, 28),
                }
            ],
            "spans": [
                {
                    "span_id": "bundle-span-1",
                    "insight_id": "miner-insight-1",
                    "raw_text": "太抽象了",
                    "normalized_text": "太抽象了",
                    "span_type": "template_core",
                    "char_start": 2,
                    "char_end": 6,
                    "confidence": 0.88,
                    "is_primary": True,
                    "query_priority": "high",
                    "reason": "评论核心评价语。",
                },
                {
                    "span_id": "bundle-span-2",
                    "insight_id": "miner-insight-1",
                    "raw_text": "圣经",
                    "normalized_text": "圣经",
                    "span_type": "context_term",
                    "char_start": 9,
                    "char_end": 11,
                    "confidence": 0.63,
                    "is_primary": False,
                    "query_priority": "medium",
                    "reason": "圈层语境词。",
                },
            ],
            "hypotheses": [
                {
                    "hypothesis_id": "bundle-hyp-1",
                    "insight_id": "miner-insight-1",
                    "candidate_title": "太抽象了",
                    "hypothesis_type": "template_meme",
                    "miner_opinion": "模板性评价语更可能是梗核。",
                    "support_score": 0.8,
                    "counter_score": 0.1,
                    "uncertainty_score": 0.2,
                    "suggested_action": "search_then_review",
                    "status": "queued",
                }
            ],
            "hypothesis_spans": [
                {
                    "hypothesis_id": "bundle-hyp-1",
                    "span_id": "bundle-span-1",
                    "role": "primary",
                },
                {
                    "hypothesis_id": "bundle-hyp-1",
                    "span_id": "bundle-span-2",
                    "role": "related",
                },
            ],
            "evidences": [
                {
                    "evidence_id": "bundle-ev-1",
                    "hypothesis_id": "bundle-hyp-1",
                    "span_id": "bundle-span-1",
                    "query": "太抽象了",
                    "query_mode": "literal",
                    "source_kind": "web_search_summary",
                    "source_title": "测试搜索结果",
                    "source_url": "https://example.com/meme",
                    "snippet": "常被用作对内容离谱程度的概括。",
                    "evidence_direction": "supports_template",
                    "evidence_strength": 0.76,
                }
            ],
            "miner_summary": {
                "recommended_hypothesis_id": "bundle-hyp-1",
                "should_queue_for_research": True,
                "reason": "模板句的复用性最强。",
            },
        },
    )
    upsert_research_decision(
        conn,
        {
            "decision_id": "decision-researched",
            "bundle_id": "bundle-researched",
            "target_hypothesis_id": "bundle-hyp-1",
            "decision": "accept",
            "final_title": "太抽象了",
            "target_record_id": "太抽象了",
            "confidence": 0.85,
            "reason": "模板化评价语比上下文词更适合作为词条名。",
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
                "id": "太抽象了",
                "title": "太抽象了",
                "alias": [],
                "definition": "用于概括内容离谱、混乱或超出预期的评价语。",
                "origin": "常见于 B 站评论区的抽象内容评价语境。",
                "category": ["抽象"],
                "platform": "Bilibili",
                "heat_index": 72,
                "lifecycle_stage": "peak",
                "first_detected_at": date(2026, 3, 28),
                "source_urls": ["https://example.com/meme"],
                "confidence_score": 0.85,
                "human_verified": False,
                "updated_at": date(2026, 3, 28),
            },
        },
        persist_record=True,
    )
    upsert_comment_bundle(
        conn,
        {
            "bundle_id": "bundle-pending",
            "insight": {
                "insight_id": "bundle-insight-2",
                "bvid": "BV1raw222",
                "collected_date": date(2026, 3, 28),
                "comment_text": "今天也要电子榨菜",
                "worth_investigating": True,
                "signal_score": 0.67,
                "reason": "句子更像稳定复用表达，待进一步研判。",
                "status": "bundled",
            },
            "video_refs": [
                {
                    "bvid": "BV1raw222",
                    "title": "第二条 Scout 快照",
                    "url": "https://www.bilibili.com/video/BV1raw222",
                    "partition": "鬼畜",
                    "collected_date": date(2026, 3, 28),
                }
            ],
            "spans": [
                {
                    "span_id": "bundle2-span-1",
                    "insight_id": "bundle-insight-2",
                    "raw_text": "电子榨菜",
                    "normalized_text": "电子榨菜",
                    "span_type": "quote_core",
                    "char_start": 5,
                    "char_end": 9,
                    "confidence": 0.78,
                    "is_primary": True,
                    "query_priority": "high",
                    "reason": "可能是稳定复用短语。",
                }
            ],
            "hypotheses": [
                {
                    "hypothesis_id": "bundle2-hyp-1",
                    "insight_id": "bundle-insight-2",
                    "candidate_title": "电子榨菜",
                    "hypothesis_type": "quote_meme",
                    "miner_opinion": "短语本身可能具有独立传播性。",
                    "support_score": 0.65,
                    "counter_score": 0.15,
                    "uncertainty_score": 0.35,
                    "suggested_action": "search_then_review",
                    "status": "queued",
                }
            ],
            "hypothesis_spans": [
                {
                    "hypothesis_id": "bundle2-hyp-1",
                    "span_id": "bundle2-span-1",
                    "role": "primary",
                }
            ],
            "evidences": [
                {
                    "evidence_id": "bundle2-ev-1",
                    "hypothesis_id": "bundle2-hyp-1",
                    "span_id": "bundle2-span-1",
                    "query": "电子榨菜",
                    "query_mode": "literal",
                    "source_kind": "web_search_result",
                    "source_title": "测试线索",
                    "source_url": "https://example.com/snack",
                    "snippet": "常指下饭视频或陪伴式内容。",
                    "evidence_direction": "supports_meme",
                    "evidence_strength": 0.61,
                }
            ],
            "miner_summary": {
                "recommended_hypothesis_id": "bundle2-hyp-1",
                "should_queue_for_research": True,
                "reason": "证据已有雏形，但还需要最终判定。",
            },
        },
    )
    finish_pipeline_run(
        conn,
        run_id,
        status="success",
        result_count=2,
        summary="采集 2 个视频快照",
        payload={"video_count": 2},
    )
    finish_pipeline_run(
        conn,
        miner_run_id,
        status="success",
        result_count=1,
        summary="写入 1 条评论线索，高价值 1 条",
        payload={
            "target_date": "2026-03-28",
            "video_count": 1,
            "insight_count": 1,
            "high_value_count": 1,
        },
    )
    conversation_id = create_agent_conversation(
        conn,
        run_id=research_run_id,
        agent_name="researcher",
        word="抽象圣经",
    )
    finish_agent_conversation(
        conn,
        conversation_id,
        status="success",
        summary="完成词条分析",
        messages_json='[{"kind":"request"},{"kind":"response"}]',
        message_count=2,
        output_json='{"title":"抽象圣经","definition":"测试输出"}',
    )
    miner_conversation_id = create_agent_conversation(
        conn,
        run_id=miner_run_id,
        agent_name="miner",
        word="BV1raw111",
    )
    finish_agent_conversation(
        conn,
        miner_conversation_id,
        status="success",
        summary="BV1raw111 评论初筛完成，2 条评论，1 条高价值",
        messages_json='[{"role":"system"},{"role":"user"},{"role":"assistant"}]',
        message_count=3,
        output_json='{"bvid":"BV1raw111","title":"第一条 Scout 快照"}',
    )
    conn.close()

    root_page = client.get("/")
    assert root_page.status_code == 200
    assert "MemeDetector Workbench" in root_page.text

    removed_workbench = client.get("/workbench")
    assert removed_workbench.status_code == 404

    removed_workbench_candidates = client.get("/workbench/candidates")
    assert removed_workbench_candidates.status_code == 404

    removed_admin = client.get("/admin")
    assert removed_admin.status_code == 404

    removed_admin_candidates = client.get("/admin/candidates")
    assert removed_admin_candidates.status_code == 404

    removed_candidates = client.get("/candidates")
    assert removed_candidates.status_code == 404

    runs_resp = client.get("/api/v1/runs")
    assert runs_resp.status_code == 200
    body = runs_resp.json()
    assert len(body) == 3
    assert any(item["job_name"] == "scout" for item in body)
    assert any(item["job_name"] == "miner" for item in body)
    assert any(item["job_name"] == "research" for item in body)
    scout_run = next(item for item in body if item["job_name"] == "scout")
    assert scout_run["summary"] == "采集 2 个视频快照"

    jobs_resp = client.get("/api/v1/jobs")
    assert jobs_resp.status_code == 200
    jobs_payload = jobs_resp.json()
    assert jobs_payload[0]["id"] == "daily_scout"
    assert any(item["id"] == "daily_miner_insights" for item in jobs_payload)
    assert any(item["id"] == "daily_miner_bundles" for item in jobs_payload)
    assert all("is_running" in item for item in jobs_payload)
    assert all("active_phase" in item for item in jobs_payload)
    assert all("active_progress_current" in item for item in jobs_payload)
    assert all("active_progress_total" in item for item in jobs_payload)
    assert all("active_progress_message" in item for item in jobs_payload)

    scout_raw_resp = client.get("/api/v1/scout/raw-videos?limit=10&offset=0")
    assert scout_raw_resp.status_code == 200
    scout_raw_payload = scout_raw_resp.json()
    assert scout_raw_payload["total"] == 2
    assert scout_raw_payload["items"][0]["bvid"].startswith("BV1raw")
    assert scout_raw_payload["items"][0]["first_comment"] != ""
    assert scout_raw_payload["items"][0]["picture_count"] >= 0
    assert scout_raw_payload["items"][0]["pipeline_stage"] == "scouted"

    scout_raw_detail_resp = client.get("/api/v1/scout/raw-videos/BV1raw111?collected_date=2026-03-28")
    assert scout_raw_detail_resp.status_code == 200
    scout_raw_detail = scout_raw_detail_resp.json()
    assert scout_raw_detail["title"] == "第一条 Scout 快照"
    assert scout_raw_detail["tags"] == ["整活", "抽象", "二创"]
    assert scout_raw_detail["comments"] == ["第一条评论", "第二条评论"]
    assert scout_raw_detail["picture_count"] == 1
    assert scout_raw_detail["comments_with_pictures"] == 1
    assert scout_raw_detail["comment_snapshots"][0]["pictures"][0]["asset_id"] == "test-asset"

    stage_promote_resp = client.post(
        "/api/v1/scout/raw-videos/BV1raw111/stage",
        json={"collected_date": "2026-03-28", "stage": "researched"},
    )
    assert stage_promote_resp.status_code == 200
    stage_promote_payload = stage_promote_resp.json()
    assert stage_promote_payload["pipeline_stage"] == "researched"
    assert stage_promote_payload["miner_status"] == "processed"
    assert stage_promote_payload["research_status"] == "processed"
    assert stage_promote_payload["affected_insight_count"] == 1

    promoted_miner_detail = client.get("/api/v1/miner/comment-insights/miner-insight-1")
    assert promoted_miner_detail.status_code == 200
    assert promoted_miner_detail.json()["status"] == "bundled"

    stage_reset_resp = client.post(
        "/api/v1/scout/raw-videos/BV1raw111/stage",
        json={"collected_date": "2026-03-28", "stage": "scouted"},
    )
    assert stage_reset_resp.status_code == 200
    stage_reset_payload = stage_reset_resp.json()
    assert stage_reset_payload["pipeline_stage"] == "scouted"
    assert stage_reset_payload["miner_status"] == "pending"
    assert stage_reset_payload["research_status"] == "pending"
    assert stage_reset_payload["affected_insight_count"] == 1

    reset_miner_detail = client.get("/api/v1/miner/comment-insights/miner-insight-1")
    assert reset_miner_detail.status_code == 200
    assert reset_miner_detail.json()["status"] == "pending_bundle"

    media_asset_resp = client.get("/api/v1/media-assets/test-asset")
    assert media_asset_resp.status_code == 200
    assert media_asset_resp.json()["mime_type"] == "image/png"

    media_content_resp = client.get("/api/v1/media-assets/test-asset/content")
    assert media_content_resp.status_code == 200
    assert media_content_resp.headers["content-type"] == "image/png"
    assert media_content_resp.content.startswith(b"\x89PNG")

    miner_resp = client.get("/api/v1/miner/comment-insights?limit=10&offset=0")
    assert miner_resp.status_code == 200
    miner_payload = miner_resp.json()
    assert miner_payload["total"] == 1
    assert miner_payload["items"][0]["insight_id"] == "miner-insight-1"
    assert miner_payload["items"][0]["is_meme_candidate"] is True

    miner_detail_resp = client.get("/api/v1/miner/comment-insights/miner-insight-1")
    assert miner_detail_resp.status_code == 200
    miner_detail = miner_detail_resp.json()
    assert miner_detail["bvid"] == "BV1raw111"
    assert miner_detail["video_context"]["summary"] == "视频在讲抽象整活。"

    bundles_resp = client.get("/api/v1/research/bundles/page?limit=10&offset=0")
    assert bundles_resp.status_code == 200
    bundles_payload = bundles_resp.json()
    assert bundles_payload["total"] == 2
    assert {item["bundle_id"] for item in bundles_payload["items"]} == {
        "bundle-pending",
        "bundle-researched",
    }

    bundled_only_resp = client.get("/api/v1/research/bundles/page?status=bundled&limit=10&offset=0")
    assert bundled_only_resp.status_code == 200
    bundled_only_payload = bundled_only_resp.json()
    assert bundled_only_payload["total"] == 1
    assert bundled_only_payload["items"][0]["bundle_id"] == "bundle-pending"

    queued_only_resp = client.get("/api/v1/research/bundles/page?status=bundled&queued_only=true&limit=10&offset=0")
    assert queued_only_resp.status_code == 200
    queued_only_payload = queued_only_resp.json()
    assert queued_only_payload["total"] == 1
    assert queued_only_payload["items"][0]["bundle_id"] == "bundle-pending"

    bundle_detail_resp = client.get("/api/v1/research/bundles/bundle-researched")
    assert bundle_detail_resp.status_code == 200
    bundle_detail = bundle_detail_resp.json()
    assert bundle_detail["bundle"]["bundle_id"] == "bundle-researched"
    assert bundle_detail["bundle"]["insight"]["status"] == "researched"
    assert bundle_detail["bundle"]["hypotheses"][0]["candidate_title"] == "太抽象了"
    assert bundle_detail["bundle"]["evidences"][0]["query"] == "太抽象了"
    assert bundle_detail["decisions"][0]["decision"] == "accept"
    assert bundle_detail["decisions"][0]["final_title"] == "太抽象了"

    stats_resp = client.get("/api/v1/stats")
    assert stats_resp.status_code == 200
    stats_payload = stats_resp.json()
    assert stats_payload["bundles"] == {"total": 2, "bundled": 1, "researched": 1, "ready": 1}
    assert stats_payload["blockers"] == {"pending_miner_videos": 2, "failed_miner_videos": 0}
    assert stats_payload["memes_in_library"] == 2

    conversations_resp = client.get("/api/v1/agent-conversations?limit=10&offset=0")
    assert conversations_resp.status_code == 200
    conversations_body = conversations_resp.json()
    assert conversations_body["total"] == 2
    assert {item["agent_name"] for item in conversations_body["items"]} == {"miner", "researcher"}

    miner_conversations_resp = client.get("/api/v1/agent-conversations?limit=10&offset=0&agent_name=miner")
    assert miner_conversations_resp.status_code == 200
    miner_conversations_body = miner_conversations_resp.json()
    assert miner_conversations_body["total"] == 1
    assert miner_conversations_body["items"][0]["word"] == "BV1raw111"
    assert miner_conversations_body["items"][0]["agent_name"] == "miner"

    conversation_detail_resp = client.get(f"/api/v1/agent-conversations/{conversation_id}")
    assert conversation_detail_resp.status_code == 200
    conversation_detail = conversation_detail_resp.json()
    assert conversation_detail["message_count"] == 2
    assert conversation_detail["output"]["title"] == "抽象圣经"

    conversation_trace_resp = client.get(f"/api/v1/agent-conversations/{conversation_id}/trace")
    assert conversation_trace_resp.status_code == 200
    conversation_trace = conversation_trace_resp.json()
    assert conversation_trace["conversation"]["id"] == conversation_id
    assert conversation_trace["steps"] == []


def test_jobs_run_endpoint(client, monkeypatch):
    called = {}

    async def fake_start_background_job(job_name: str, *, trigger_mode: str = "manual") -> dict:
        called["job_name"] = job_name
        called["trigger_mode"] = trigger_mode
        return {
            "job_name": job_name,
            "started": True,
            "message": f"{job_name} 已在后台启动",
            "runtime_state": {
                "running": True,
                "trigger_mode": trigger_mode,
                "started_at": None,
                "last_started_at": None,
                "last_finished_at": None,
                "last_error": "",
            },
        }

    monkeypatch.setattr("meme_detector.api.routes.start_background_job", fake_start_background_job)

    response = client.post("/api/v1/jobs/miner/run")

    assert response.status_code == 200
    payload = response.json()
    assert payload["started"] is True
    assert payload["job_name"] == "miner"
    assert called == {"job_name": "miner", "trigger_mode": "manual"}


def test_research_bundle_detail_allows_legacy_bundle_without_primary_span_link(client):
    conn = get_conn()
    upsert_comment_bundle(
        conn,
        {
            "bundle_id": "bundle-legacy",
            "insight": {
                "insight_id": "bundle-legacy-insight",
                "bvid": "BV1LEGACY01",
                "collected_date": date(2026, 3, 28),
                "comment_text": "这也太抽象了",
                "worth_investigating": True,
                "signal_score": 0.88,
                "reason": "旧数据缺少 primary link。",
                "status": "bundled",
            },
            "video_refs": [
                {
                    "bvid": "BV1LEGACY01",
                    "title": "旧证据包测试视频",
                    "url": "https://www.bilibili.com/video/BV1LEGACY01",
                    "partition": "动画",
                    "collected_date": date(2026, 3, 28),
                }
            ],
            "spans": [
                {
                    "span_id": "legacy-span-1",
                    "insight_id": "bundle-legacy-insight",
                    "raw_text": "太抽象了",
                    "normalized_text": "太抽象了",
                    "span_type": "template_core",
                    "char_start": 2,
                    "char_end": 6,
                    "confidence": 0.9,
                    "is_primary": True,
                    "query_priority": "high",
                    "reason": "主模板句。",
                }
            ],
            "hypotheses": [
                {
                    "hypothesis_id": "legacy-hyp-1",
                    "insight_id": "bundle-legacy-insight",
                    "candidate_title": "太抽象了",
                    "hypothesis_type": "template_meme",
                    "miner_opinion": "主模板句就是传播核心。",
                    "support_score": 0.8,
                    "counter_score": 0.1,
                    "uncertainty_score": 0.2,
                    "suggested_action": "search_then_review",
                    "status": "queued",
                }
            ],
            "hypothesis_spans": [
                {
                    "hypothesis_id": "legacy-hyp-1",
                    "span_id": "legacy-span-1",
                    "role": "primary",
                }
            ],
            "evidences": [],
            "miner_summary": {
                "recommended_hypothesis_id": "legacy-hyp-1",
                "should_queue_for_research": True,
                "reason": "旧 bundle 兼容读取。",
            },
        },
    )
    conn.execute("DELETE FROM hypothesis_spans WHERE hypothesis_id = ?", ["legacy-hyp-1"])
    conn.close()

    response = client.get("/api/v1/research/bundles/bundle-legacy")

    assert response.status_code == 200
    payload = response.json()
    primary_links = [
        item
        for item in payload["bundle"]["hypothesis_spans"]
        if item["hypothesis_id"] == "legacy-hyp-1" and item["role"] == "primary"
    ]
    assert len(primary_links) == 1
    assert primary_links[0]["span_id"] == "legacy-span-1"
