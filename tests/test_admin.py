import pytest
from fastapi.testclient import TestClient

from meme_detector.archivist.duckdb_store import (
    create_pipeline_run,
    finish_pipeline_run,
    get_conn,
    upsert_scout_candidates,
)


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "meme_detector.archivist.duckdb_store.settings.duckdb_path",
        str(tmp_path / "admin-test.db"),
    )
    monkeypatch.setattr("meme_detector.api.app.ensure_index", lambda: None)

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
                "name": "每日采集与词频统计",
                "next_run_time": "2026-03-28T02:05:00",
                "trigger": "cron[hour='2', minute='5']",
            }
        ],
    )

    from meme_detector.api.app import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


def test_admin_page_and_runs_api(client):
    conn = get_conn()
    run_id = create_pipeline_run(conn, job_name="scout", trigger_mode="manual")
    upsert_scout_candidates(
        conn,
        [
            {
                "phrase": "抽象圣经",
                "explanation": "来自评论区复读的抽象短语",
                "examples": ["这也太抽象了", "全是圣经"],
                "confidence": 0.92,
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
    conn.close()

    admin_page = client.get("/admin")
    assert admin_page.status_code == 200
    assert "MemeDetector 控制台" in admin_page.text
    assert "/admin/candidates" in admin_page.text

    candidates_page = client.get("/admin/candidates")
    assert candidates_page.status_code == 200
    assert "候选梗队列" in candidates_page.text

    runs_resp = client.get("/api/v1/runs")
    assert runs_resp.status_code == 200
    body = runs_resp.json()
    assert len(body) == 1
    assert body[0]["job_name"] == "scout"
    assert body[0]["summary"] == "发现 2 个候选梗"

    jobs_resp = client.get("/api/v1/jobs")
    assert jobs_resp.status_code == 200
    assert jobs_resp.json()[0]["id"] == "daily_scout"

    candidates_resp = client.get("/api/v1/candidates/page?limit=1&offset=0")
    assert candidates_resp.status_code == 200
    payload = candidates_resp.json()
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    assert payload["items"][0]["explanation"] != ""

    delete_resp = client.delete("/api/v1/candidates")
    assert delete_resp.status_code == 200
    assert delete_resp.json()["deleted_count"] == 2

    candidates_after = client.get("/api/v1/candidates/page?limit=10&offset=0")
    assert candidates_after.status_code == 200
    assert candidates_after.json()["total"] == 0
