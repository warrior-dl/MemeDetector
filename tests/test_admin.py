from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from meme_detector.archivist.duckdb_store import (
    create_agent_conversation,
    create_pipeline_run,
    finish_agent_conversation,
    finish_pipeline_run,
    get_conn,
    upsert_miner_comment_insights,
    upsert_scout_candidates,
    upsert_scout_raw_videos,
)


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "admin-test.db"),
    )
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.media_asset_root",
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
        "meme_detector.archivist.duckdb_store._download_media_asset",
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
                "id": "daily_miner",
                "name": "每日评论线索挖掘",
                "next_run_time": "2026-03-28T03:00:00",
                "trigger": "cron[hour='3', minute='0']",
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
                        "content": {"message": "第一条评论", "pictures": [{"img_src": "https://example.com/comment-1.png"}]},
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
    upsert_scout_candidates(
        conn,
        [
            {
                "word": "抽象圣经",
                "score": 92.0,
                "is_new_word": True,
                "sample_comments": "- 这也太抽象了，全是圣经",
                "explanation": "来自评论区复读的抽象短语",
                "video_refs": [
                    {
                        "bvid": "BV1raw111",
                        "partition": "动画",
                        "title": "第一条 Scout 快照",
                        "description": "原始描述 A",
                        "url": "https://www.bilibili.com/video/BV1raw111",
                        "tags": ["整活", "抽象", "二创"],
                        "matched_comment_count": 1,
                        "matched_comments": ["这也太抽象了，全是圣经"],
                    }
                ],
            },
            {
                "phrase": "电子榨菜",
                "explanation": "下饭视频相关表述",
                "examples": ["今天也要电子榨菜"],
                "confidence": 0.76,
            },
        ],
    )
    finish_pipeline_run(
        conn,
        run_id,
        status="success",
        result_count=2,
        summary="发现 2 个候选梗",
        payload={"candidate_count": 2},
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

    admin_page = client.get("/admin")
    assert admin_page.status_code == 200
    assert "MemeDetector 控制台" in admin_page.text
    assert "/admin/scout" in admin_page.text
    assert "/admin/miner" in admin_page.text
    assert "/admin/candidates" in admin_page.text
    assert "/admin/conversations" in admin_page.text

    scout_page = client.get("/admin/scout")
    assert scout_page.status_code == 200
    assert "Scout 原始数据" in scout_page.text

    miner_page = client.get("/admin/miner")
    assert miner_page.status_code == 200
    assert "Miner 评论线索" in miner_page.text

    candidates_page = client.get("/admin/candidates")
    assert candidates_page.status_code == 200
    assert "候选梗队列" in candidates_page.text

    candidate_sources_page = client.get("/admin/candidate-sources?word=%E6%8A%BD%E8%B1%A1%E5%9C%A3%E7%BB%8F")
    assert candidate_sources_page.status_code == 200
    assert "候选梗来源线索" in candidate_sources_page.text

    conversations_page = client.get("/admin/conversations")
    assert conversations_page.status_code == 200
    assert "Agent 对话记录" in conversations_page.text

    runs_resp = client.get("/api/v1/runs")
    assert runs_resp.status_code == 200
    body = runs_resp.json()
    assert len(body) == 3
    assert any(item["job_name"] == "scout" for item in body)
    assert any(item["job_name"] == "miner" for item in body)
    assert any(item["job_name"] == "research" for item in body)
    scout_run = next(item for item in body if item["job_name"] == "scout")
    assert scout_run["summary"] == "发现 2 个候选梗"

    jobs_resp = client.get("/api/v1/jobs")
    assert jobs_resp.status_code == 200
    jobs_payload = jobs_resp.json()
    assert jobs_payload[0]["id"] == "daily_scout"
    assert any(item["id"] == "daily_miner" for item in jobs_payload)
    assert all("is_running" in item for item in jobs_payload)

    scout_raw_resp = client.get("/api/v1/scout/raw-videos?limit=10&offset=0")
    assert scout_raw_resp.status_code == 200
    scout_raw_payload = scout_raw_resp.json()
    assert scout_raw_payload["total"] == 2
    assert scout_raw_payload["items"][0]["bvid"].startswith("BV1raw")
    assert scout_raw_payload["items"][0]["first_comment"] != ""
    assert scout_raw_payload["items"][0]["picture_count"] >= 0
    assert scout_raw_payload["items"][0]["pipeline_stage"] == "scouted"

    scout_raw_detail_resp = client.get(
        "/api/v1/scout/raw-videos/BV1raw111?collected_date=2026-03-28"
    )
    assert scout_raw_detail_resp.status_code == 200
    scout_raw_detail = scout_raw_detail_resp.json()
    assert scout_raw_detail["title"] == "第一条 Scout 快照"
    assert scout_raw_detail["tags"] == ["整活", "抽象", "二创"]
    assert scout_raw_detail["comments"] == ["第一条评论", "第二条评论"]
    assert scout_raw_detail["picture_count"] == 1
    assert scout_raw_detail["comments_with_pictures"] == 1
    assert scout_raw_detail["comment_snapshots"][0]["pictures"][0]["asset_id"] == "test-asset"

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

    candidates_resp = client.get("/api/v1/candidates/page?limit=1&offset=0")
    assert candidates_resp.status_code == 200
    payload = candidates_resp.json()
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    assert payload["items"][0]["explanation"] != ""

    candidate_sources_resp = client.get(
        "/api/v1/candidates/%E6%8A%BD%E8%B1%A1%E5%9C%A3%E7%BB%8F/sources"
    )
    assert candidate_sources_resp.status_code == 200
    candidate_sources = candidate_sources_resp.json()
    assert candidate_sources["candidate"]["word"] == "抽象圣经"
    assert candidate_sources["video_refs"][0]["bvid"] == "BV1raw111"
    assert candidate_sources["source_insights"][0]["insight_id"] == "miner-insight-1"

    delete_resp = client.delete("/api/v1/candidates")
    assert delete_resp.status_code == 200
    assert delete_resp.json()["deleted_count"] == 2

    candidates_after = client.get("/api/v1/candidates/page?limit=10&offset=0")
    assert candidates_after.status_code == 200
    assert candidates_after.json()["total"] == 0

    conversations_resp = client.get("/api/v1/agent-conversations?limit=10&offset=0")
    assert conversations_resp.status_code == 200
    conversations_body = conversations_resp.json()
    assert conversations_body["total"] == 2
    assert {item["agent_name"] for item in conversations_body["items"]} == {"miner", "researcher"}

    miner_conversations_resp = client.get(
        "/api/v1/agent-conversations?limit=10&offset=0&agent_name=miner"
    )
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
