"""
REST API 路由。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from meme_detector.archivist.duckdb_store import (
    delete_all_candidates,
    get_candidates,
    get_candidates_page,
    get_conn,
    get_pipeline_run,
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
    job_name: str | None = Query(None, description="任务名称：scout / research"),
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
