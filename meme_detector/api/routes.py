"""
REST API 路由。
"""

from __future__ import annotations

from contextlib import closing
from datetime import date
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from fastapi.responses import FileResponse

from meme_detector.archivist.duckdb_store import (
    get_agent_conversation,
    get_comment_bundle_detail,
    get_comment_bundles_page,
    get_conn,
    get_media_asset,
    get_miner_comment_insight,
    get_miner_comment_insights_page,
    get_pipeline_run,
    get_scout_raw_video,
    get_scout_raw_videos_page,
    list_agent_conversations,
    list_pipeline_runs,
    update_scout_raw_video_stage,
)
from meme_detector.archivist.meili_store import (
    get_meme,
    search_memes,
    update_human_verified,
)
from meme_detector.pipeline_service import (
    JOB_NAMES,
    get_all_job_runtime_states,
    start_background_job,
)
from meme_detector.scheduler import get_scheduler_jobs

router = APIRouter()


class ScoutRawVideoStageUpdateRequest(BaseModel):
    collected_date: date
    stage: str


def _run_with_conn(callback):
    with closing(get_conn()) as conn:
        return callback(conn)


# ── 梗库检索 ─────────────────────────────────────────────────

@router.get("/memes", summary="梗列表（支持过滤、排序、分页）")
async def list_memes(
    category: str | None = Query(None, description="分类过滤，如：抽象"),
    lifecycle: str | None = Query(None, description="生命周期：emerging/peak/declining"),
    verified_only: bool = Query(False, description="仅返回人工验证的梗"),
    sort_by: str = Query("heat_index:desc", description="排序字段"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    filters_parts = []
    if category:
        filters_parts.append(f'category = "{category}"')
    if lifecycle:
        filters_parts.append(f'lifecycle_stage = "{lifecycle}"')
    if verified_only:
        filters_parts.append("human_verified = true")

    filters = " AND ".join(filters_parts) if filters_parts else None
    return await search_memes("", filters=filters, sort=[sort_by], limit=limit, offset=offset)


@router.get("/memes/search", summary="全文检索梗")
async def full_text_search(
    q: str = Query(..., description="搜索关键词"),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> dict:
    return await search_memes(q, limit=limit, offset=offset)


@router.get("/memes/{meme_id}", summary="获取单个梗详情")
async def get_meme_detail(meme_id: str) -> dict:
    record = await get_meme(meme_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"梗 '{meme_id}' 不存在")
    return record


# ── Scout 原始数据（内部使用）─────────────────────────────────

@router.get("/scout/raw-videos", summary="分页获取 Scout 原始视频快照")
async def list_scout_raw_videos(
    research_status: str | None = Query(
        None,
        description="Research 状态：pending / processed，不传则返回全部",
    ),
    partition: str | None = Query(None, description="分区关键字过滤"),
    keyword: str | None = Query(None, description="标题 / 描述 / BVID 关键字过滤"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    return _run_with_conn(
        lambda conn: get_scout_raw_videos_page(
            conn,
            research_status=research_status,
            partition=partition,
            keyword=keyword,
            limit=limit,
            offset=offset,
        )
    )


@router.get("/scout/raw-videos/{bvid}", summary="获取单个 Scout 原始视频快照详情")
async def get_scout_raw_video_detail(
    bvid: str,
    collected_date: date = Query(..., description="采集日期，格式 YYYY-MM-DD"),
) -> dict:
    snapshot = _run_with_conn(
        lambda conn: get_scout_raw_video(conn, bvid=bvid, collected_date=collected_date)
    )
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"原始快照 '{bvid}@{collected_date}' 不存在")
    return snapshot


@router.post("/scout/raw-videos/{bvid}/stage", summary="手动更新 Scout 原始视频阶段")
async def set_scout_raw_video_stage(
    bvid: str,
    payload: ScoutRawVideoStageUpdateRequest,
) -> dict:
    if payload.stage not in {"scouted", "mined", "researched"}:
        raise HTTPException(status_code=400, detail="stage 必须为 scouted、mined 或 researched")

    snapshot = _run_with_conn(
        lambda conn: update_scout_raw_video_stage(
            conn,
            bvid=bvid,
            collected_date=payload.collected_date,
            stage=payload.stage,
        )
    )
    if not snapshot:
        raise HTTPException(
            status_code=404,
            detail=f"原始快照 '{bvid}@{payload.collected_date}' 不存在",
        )
    return snapshot


# ── Miner 评论线索（内部使用）─────────────────────────────────

@router.get("/miner/comment-insights", summary="分页获取 Miner 评论线索")
async def list_miner_comment_insights(
    status: str | None = Query(
        None,
        description="状态过滤：pending_bundle / bundling / bundled / bundle_failed / discarded",
    ),
    keyword: str | None = Query(None, description="标题 / 简介 / 评论 / 理由关键字过滤"),
    bvid: str | None = Query(None, description="BVID 关键字过滤"),
    only_meme_candidates: bool = Query(False, description="仅返回潜在梗评论"),
    only_insider_knowledge: bool = Query(False, description="仅返回圈内知识评论"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    return _run_with_conn(
        lambda conn: get_miner_comment_insights_page(
            conn,
            status=status,
            keyword=keyword,
            bvid=bvid,
            only_meme_candidates=only_meme_candidates,
            only_insider_knowledge=only_insider_knowledge,
            limit=limit,
            offset=offset,
        )
    )


@router.get("/miner/comment-insights/{insight_id}", summary="获取单条 Miner 评论线索详情")
async def get_miner_comment_insight_detail(insight_id: str) -> dict:
    insight = _run_with_conn(lambda conn: get_miner_comment_insight(conn, insight_id))
    if not insight:
        raise HTTPException(status_code=404, detail=f"评论线索 '{insight_id}' 不存在")
    return insight


@router.get("/research/bundles/page", summary="分页获取评论证据包摘要")
async def list_research_bundles(
    status: str | None = Query(None, description="状态过滤：bundled / researched"),
    queued_only: bool = Query(False, description="仅返回存在 queued hypothesis 的证据包"),
    keyword: str | None = Query(None, description="评论 / 原因 / BVID 关键字过滤"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    return _run_with_conn(
        lambda conn: get_comment_bundles_page(
            conn,
            status=status,
            queued_only=queued_only,
            keyword=keyword,
            limit=limit,
            offset=offset,
        )
    )


@router.get("/research/bundles/{bundle_id}", summary="获取评论证据包详情")
async def get_research_bundle_detail(bundle_id: str) -> dict:
    bundle = _run_with_conn(lambda conn: get_comment_bundle_detail(conn, bundle_id))
    if not bundle:
        raise HTTPException(status_code=404, detail=f"评论证据包 '{bundle_id}' 不存在")
    return bundle


@router.get("/media-assets/{asset_id}", summary="获取媒体资产元数据")
async def get_media_asset_detail(asset_id: str) -> dict:
    asset = _run_with_conn(lambda conn: get_media_asset(conn, asset_id))
    if not asset:
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 不存在")
    return asset


@router.get("/media-assets/{asset_id}/content", summary="读取媒体资产文件")
async def get_media_asset_content(asset_id: str) -> FileResponse:
    asset = _run_with_conn(lambda conn: get_media_asset(conn, asset_id))
    if not asset:
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 不存在")
    storage_path = asset.get("storage_path") or ""
    if not storage_path:
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 没有本地文件")
    path = Path(storage_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 的本地文件不存在")
    return FileResponse(path, media_type=asset.get("mime_type") or None)


@router.post("/memes/{meme_id}/verify", summary="标记梗为人工验证")
async def mark_verified(meme_id: str, verified: bool = True) -> dict:
    ok = await update_human_verified(meme_id, verified)
    if not ok:
        raise HTTPException(status_code=404, detail=f"梗 '{meme_id}' 不存在")
    return {"id": meme_id, "human_verified": verified}


# ── 运行记录 / 调度概览 ─────────────────────────────────────

@router.get("/runs", summary="获取任务运行记录")
async def list_runs(
    job_name: str | None = Query(
        None,
        description="任务名称：scout / miner_insights / miner_bundles / research",
    ),
    status: str | None = Query(None, description="运行状态：running / success / failed"),
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    return _run_with_conn(
        lambda conn: list_pipeline_runs(conn, job_name=job_name, status=status, limit=limit)
    )


@router.get("/runs/{run_id}", summary="获取单次任务运行详情")
async def get_run_detail(run_id: str) -> dict:
    run = _run_with_conn(lambda conn: get_pipeline_run(conn, run_id))
    if not run:
        raise HTTPException(status_code=404, detail=f"运行记录 '{run_id}' 不存在")
    return run


@router.get("/jobs", summary="获取调度任务概览")
async def list_jobs() -> list[dict]:
    runtime_states = get_all_job_runtime_states()
    job_name_by_scheduler_id = {
        "daily_scout": "scout",
        "daily_miner_insights": "miner_insights",
        "daily_miner_bundles": "miner_bundles",
        "weekly_research": "research",
    }
    jobs = []
    for job in get_scheduler_jobs():
        mapped_job_name = job_name_by_scheduler_id.get(job.get("id", ""))
        runtime_state = runtime_states.get(mapped_job_name or "", {})
        jobs.append(
            {
                **job,
                "job_name": mapped_job_name,
                "is_running": bool(runtime_state.get("running")),
                "active_trigger_mode": runtime_state.get("trigger_mode"),
                "active_started_at": runtime_state.get("started_at"),
                "last_finished_at": runtime_state.get("last_finished_at"),
                "last_error": runtime_state.get("last_error", ""),
                "active_phase": runtime_state.get("phase", ""),
                "active_progress_current": runtime_state.get("progress_current", 0),
                "active_progress_total": runtime_state.get("progress_total", 0),
                "active_progress_unit": runtime_state.get("progress_unit", ""),
                "active_progress_message": runtime_state.get("progress_message", ""),
                "active_updated_at": runtime_state.get("updated_at"),
            }
        )
    return jobs


@router.post("/jobs/{job_name}/run", summary="手动触发任务")
async def trigger_job(job_name: str) -> dict:
    normalized_job_name = job_name.strip().lower()
    if normalized_job_name not in JOB_NAMES:
        raise HTTPException(status_code=404, detail=f"未知任务 '{job_name}'")
    return await start_background_job(normalized_job_name, trigger_mode="manual")


@router.get("/agent-conversations", summary="分页获取 Agent 对话记录")
async def list_conversations(
    run_id: str | None = Query(None, description="关联的运行记录 ID"),
    agent_name: str | None = Query(None, description="Agent 名称：miner / researcher"),
    word: str | None = Query(None, description="词条关键字过滤"),
    status: str | None = Query(None, description="状态：running / success / failed"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    return _run_with_conn(
        lambda conn: list_agent_conversations(
            conn,
            run_id=run_id,
            agent_name=agent_name,
            word=word,
            status=status,
            limit=limit,
            offset=offset,
        )
    )


@router.get("/agent-conversations/{conversation_id}", summary="获取 Agent 对话详情")
async def get_conversation_detail(conversation_id: str) -> dict:
    conversation = _run_with_conn(lambda conn: get_agent_conversation(conn, conversation_id))
    if not conversation:
        raise HTTPException(status_code=404, detail=f"对话记录 '{conversation_id}' 不存在")
    return conversation


# ── 统计概览 ─────────────────────────────────────────────────

@router.get("/stats", summary="统计概览")
async def stats() -> dict:
    bundle_rows = _run_with_conn(
        lambda conn: conn.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status='bundled')    AS bundled,
                COUNT(*) FILTER (WHERE status='researched') AS researched,
                COUNT(*)                                    AS total
            FROM comment_insights
            """
        ).fetchone()
    )
    pending_miner_video_count = _run_with_conn(
        lambda conn: conn.execute(
            """
            SELECT COUNT(*)
            FROM scout_raw_videos
            WHERE miner_status = 'pending'
            """
        ).fetchone()[0]
    )
    failed_miner_video_count = _run_with_conn(
        lambda conn: conn.execute(
            """
            SELECT COUNT(*)
            FROM scout_raw_videos
            WHERE miner_status = 'failed'
            """
        ).fetchone()[0]
    )
    ready_bundle_count = _run_with_conn(
        lambda conn: conn.execute(
            """
            SELECT COUNT(DISTINCT ci.bundle_id)
            FROM comment_insights ci
            JOIN hypotheses h ON h.bundle_id = ci.bundle_id
            WHERE ci.status = 'bundled'
              AND h.status IN ('queued', 'evidenced')
            """
        ).fetchone()[0]
    )

    word_count_result = await search_memes("", limit=0)
    bundled_count = int(bundle_rows[0] or 0)
    researched_count = int(bundle_rows[1] or 0)
    total_count = int(bundle_rows[2] or 0)
    pending_miner_video_count = int(pending_miner_video_count or 0)
    failed_miner_video_count = int(failed_miner_video_count or 0)
    ready_bundle_count = int(ready_bundle_count or 0)
    return {
        "bundles": {
            "total": total_count,
            "bundled": bundled_count,
            "researched": researched_count,
            "ready": ready_bundle_count,
        },
        "blockers": {
            "pending_miner_videos": pending_miner_video_count,
            "failed_miner_videos": failed_miner_video_count,
        },
        "memes_in_library": word_count_result.get("estimatedTotalHits", 0),
    }
