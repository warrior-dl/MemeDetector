"""
REST API 路由。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from meme_detector.archivist.duckdb_store import (
    delete_all_candidates,
    get_agent_conversation,
    get_candidate_source_insights,
    get_candidates,
    get_candidates_page,
    get_conn,
    get_media_asset,
    get_miner_comment_insight,
    get_miner_comment_insights_page,
    get_pipeline_run,
    get_scout_raw_video,
    get_scout_raw_videos_page,
    list_agent_conversations,
    list_pipeline_runs,
    update_candidate_status,
)
from meme_detector.archivist.meili_store import (
    get_meme,
    search_memes,
    update_human_verified,
)
from meme_detector.scheduler import get_scheduler_jobs

router = APIRouter()


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
    candidate_status: str | None = Query(
        None,
        description="候选提取状态：pending / processed，不传则返回全部",
    ),
    partition: str | None = Query(None, description="分区关键字过滤"),
    keyword: str | None = Query(None, description="标题 / 描述 / BVID 关键字过滤"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    conn = get_conn()
    page = get_scout_raw_videos_page(
        conn,
        candidate_status=candidate_status,
        partition=partition,
        keyword=keyword,
        limit=limit,
        offset=offset,
    )
    conn.close()
    return page


@router.get("/scout/raw-videos/{bvid}", summary="获取单个 Scout 原始视频快照详情")
async def get_scout_raw_video_detail(
    bvid: str,
    collected_date: date = Query(..., description="采集日期，格式 YYYY-MM-DD"),
) -> dict:
    conn = get_conn()
    snapshot = get_scout_raw_video(conn, bvid=bvid, collected_date=collected_date)
    conn.close()
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"原始快照 '{bvid}@{collected_date}' 不存在")
    return snapshot


# ── Miner 评论线索（内部使用）─────────────────────────────────

@router.get("/miner/comment-insights", summary="分页获取 Miner 评论线索")
async def list_miner_comment_insights(
    status: str | None = Query(None, description="状态过滤：pending / processed"),
    keyword: str | None = Query(None, description="标题 / 简介 / 评论 / 理由关键字过滤"),
    bvid: str | None = Query(None, description="BVID 关键字过滤"),
    only_meme_candidates: bool = Query(False, description="仅返回潜在梗评论"),
    only_insider_knowledge: bool = Query(False, description="仅返回圈内知识评论"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    conn = get_conn()
    page = get_miner_comment_insights_page(
        conn,
        status=status,
        keyword=keyword,
        bvid=bvid,
        only_meme_candidates=only_meme_candidates,
        only_insider_knowledge=only_insider_knowledge,
        limit=limit,
        offset=offset,
    )
    conn.close()
    return page


@router.get("/miner/comment-insights/{insight_id}", summary="获取单条 Miner 评论线索详情")
async def get_miner_comment_insight_detail(insight_id: str) -> dict:
    conn = get_conn()
    insight = get_miner_comment_insight(conn, insight_id)
    conn.close()
    if not insight:
        raise HTTPException(status_code=404, detail=f"评论线索 '{insight_id}' 不存在")
    return insight


@router.get("/media-assets/{asset_id}", summary="获取媒体资产元数据")
async def get_media_asset_detail(asset_id: str) -> dict:
    conn = get_conn()
    asset = get_media_asset(conn, asset_id)
    conn.close()
    if not asset:
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 不存在")
    return asset


@router.get("/media-assets/{asset_id}/content", summary="读取媒体资产文件")
async def get_media_asset_content(asset_id: str) -> FileResponse:
    conn = get_conn()
    asset = get_media_asset(conn, asset_id)
    conn.close()
    if not asset:
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 不存在")
    storage_path = asset.get("storage_path") or ""
    if not storage_path:
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 没有本地文件")
    path = Path(storage_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"媒体资产 '{asset_id}' 的本地文件不存在")
    return FileResponse(path, media_type=asset.get("mime_type") or None)


# ── 候选词管理（内部使用）─────────────────────────────────────

@router.get("/candidates", summary="获取候选梗列表")
async def list_candidates(
    status: str | None = Query(
        None,
        description="状态过滤：pending / accepted / rejected，不传则返回全部",
    ),
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    conn = get_conn()
    candidates = get_candidates(conn, status=status, limit=limit)
    conn.close()
    return candidates


@router.get("/candidates/page", summary="分页获取候选梗完整信息")
async def list_candidates_page(
    status: str | None = Query(
        None,
        description="状态过滤：pending / accepted / rejected，不传则返回全部",
    ),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    conn = get_conn()
    page = get_candidates_page(conn, status=status, limit=limit, offset=offset)
    conn.close()
    return page


@router.get("/candidates/{word}/sources", summary="获取候选梗来源线索")
async def get_candidate_sources(
    word: str,
    limit: int = Query(100, ge=1, le=300),
) -> dict:
    conn = get_conn()
    result = get_candidate_source_insights(conn, word=word, limit=limit)
    conn.close()
    if not result:
        raise HTTPException(status_code=404, detail=f"候选词 '{word}' 不存在")
    return result


@router.delete("/candidates", summary="删除所有候选梗")
async def remove_all_candidates() -> dict:
    conn = get_conn()
    deleted_count = delete_all_candidates(conn)
    conn.close()
    return {"deleted_count": deleted_count}


@router.post("/candidates/{word}/verify", summary="人工审核候选词")
async def verify_candidate(
    word: str,
    action: str = Query(..., description="操作：accept 或 reject"),
) -> dict:
    if action not in ("accept", "reject"):
        raise HTTPException(status_code=400, detail="action 必须为 accept 或 reject")
    conn = get_conn()
    update_candidate_status(conn, word, "accepted" if action == "accept" else "rejected")
    conn.close()
    return {"word": word, "status": action + "ed"}


@router.post("/memes/{meme_id}/verify", summary="标记梗为人工验证")
async def mark_verified(meme_id: str, verified: bool = True) -> dict:
    ok = await update_human_verified(meme_id, verified)
    if not ok:
        raise HTTPException(status_code=404, detail=f"梗 '{meme_id}' 不存在")
    return {"id": meme_id, "human_verified": verified}


# ── 运行记录 / 调度概览 ─────────────────────────────────────

@router.get("/runs", summary="获取任务运行记录")
async def list_runs(
    job_name: str | None = Query(None, description="任务名称：scout / miner / research"),
    status: str | None = Query(None, description="运行状态：running / success / failed"),
    limit: int = Query(50, ge=1, le=200),
) -> list[dict]:
    conn = get_conn()
    runs = list_pipeline_runs(conn, job_name=job_name, status=status, limit=limit)
    conn.close()
    return runs


@router.get("/runs/{run_id}", summary="获取单次任务运行详情")
async def get_run_detail(run_id: str) -> dict:
    conn = get_conn()
    run = get_pipeline_run(conn, run_id)
    conn.close()
    if not run:
        raise HTTPException(status_code=404, detail=f"运行记录 '{run_id}' 不存在")
    return run


@router.get("/jobs", summary="获取调度任务概览")
async def list_jobs() -> list[dict]:
    return get_scheduler_jobs()


@router.get("/agent-conversations", summary="分页获取 Agent 对话记录")
async def list_conversations(
    run_id: str | None = Query(None, description="关联的运行记录 ID"),
    agent_name: str | None = Query(None, description="Agent 名称：miner / researcher"),
    word: str | None = Query(None, description="词条关键字过滤"),
    status: str | None = Query(None, description="状态：running / success / failed"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    conn = get_conn()
    result = list_agent_conversations(
        conn,
        run_id=run_id,
        agent_name=agent_name,
        word=word,
        status=status,
        limit=limit,
        offset=offset,
    )
    conn.close()
    return result


@router.get("/agent-conversations/{conversation_id}", summary="获取 Agent 对话详情")
async def get_conversation_detail(conversation_id: str) -> dict:
    conn = get_conn()
    conversation = get_agent_conversation(conn, conversation_id)
    conn.close()
    if not conversation:
        raise HTTPException(status_code=404, detail=f"对话记录 '{conversation_id}' 不存在")
    return conversation


# ── 统计概览 ─────────────────────────────────────────────────

@router.get("/stats", summary="统计概览")
async def stats() -> dict:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status='pending')   AS pending,
            COUNT(*) FILTER (WHERE status='accepted')  AS accepted,
            COUNT(*) FILTER (WHERE status='rejected')  AS rejected,
            COUNT(*)                                    AS total
        FROM candidates
        """
    ).fetchone()
    conn.close()

    word_count_result = await search_memes("", limit=0)
    return {
        "candidates": {
            "total": rows[3],
            "pending": rows[0],
            "accepted": rows[1],
            "rejected": rows[2],
        },
        "memes_in_library": word_count_result.get("estimatedTotalHits", 0),
    }
