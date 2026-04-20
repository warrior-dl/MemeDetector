"""
Research 领域 DuckDB 读写。
"""

from __future__ import annotations

import json
from datetime import datetime

import duckdb


def upsert_research_decision(
    conn: duckdb.DuckDBPyConnection,
    decision,
    *,
    persist_record: bool | None = None,  # noqa: ARG001 - 兼容旧调用方
) -> None:
    """写入 Research 裁决结果，并同步 hypothesis / insight 状态。"""
    from meme_detector.pipeline_models import ResearchDecision, ResearchDecisionType

    if not isinstance(decision, ResearchDecision):
        decision = ResearchDecision.model_validate(decision)

    now = datetime.now()
    conn.execute(
        """
        INSERT INTO research_decisions (
            decision_id,
            bundle_id,
            hypothesis_id,
            decision,
            final_title,
            target_record_id,
            confidence,
            reason,
            evidence_summary_json,
            assessment_json,
            record_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (decision_id) DO UPDATE
        SET bundle_id = excluded.bundle_id,
            hypothesis_id = excluded.hypothesis_id,
            decision = excluded.decision,
            final_title = excluded.final_title,
            target_record_id = excluded.target_record_id,
            confidence = excluded.confidence,
            reason = excluded.reason,
            evidence_summary_json = excluded.evidence_summary_json,
            assessment_json = excluded.assessment_json,
            record_json = excluded.record_json,
            updated_at = excluded.updated_at
        """,
        [
            decision.decision_id,
            decision.bundle_id,
            decision.target_hypothesis_id,
            decision.decision.value,
            decision.final_title,
            decision.target_record_id,
            decision.confidence,
            decision.reason,
            json.dumps(decision.evidence_summary.model_dump(mode="json"), ensure_ascii=False),
            json.dumps(decision.assessment.model_dump(mode="json"), ensure_ascii=False),
            json.dumps(decision.record.model_dump(mode="json"), ensure_ascii=False) if decision.record else "{}",
            now,
            now,
        ],
    )

    status_by_decision = {
        ResearchDecisionType.accept: "accepted",
        ResearchDecisionType.rewrite_title: "accepted",
        ResearchDecisionType.reject: "rejected",
        ResearchDecisionType.manual_review: "manual_review",
        ResearchDecisionType.merge_into_existing: "merged",
    }
    conn.execute(
        """
        UPDATE hypotheses
        SET status = ?,
            updated_at = ?
        WHERE hypothesis_id = ?
        """,
        [status_by_decision[decision.decision], now, decision.target_hypothesis_id],
    )
    conn.execute(
        """
        UPDATE comment_insights
        SET status = 'researched',
            updated_at = ?
        WHERE bundle_id = ?
        """,
        [now, decision.bundle_id],
    )
    # 梗库实体持久化由 ``meili_store.upsert_meme`` 负责；DuckDB 不再保留副本。


def get_research_decision(
    conn: duckdb.DuckDBPyConnection,
    decision_id: str,
):
    """读取单条 Research 裁决结果。"""
    from meme_detector.pipeline_models import (
        EvidenceSummary,
        ResearchAssessment,
        ResearchDecision,
    )

    row = conn.execute(
        """
        SELECT
            decision_id,
            bundle_id,
            hypothesis_id,
            decision,
            final_title,
            target_record_id,
            confidence,
            reason,
            evidence_summary_json,
            assessment_json,
            record_json
        FROM research_decisions
        WHERE decision_id = ?
        """,
        [decision_id],
    ).fetchone()
    if not row:
        return None

    record_payload = _load_json_text(row[10], default={})
    return ResearchDecision(
        decision_id=str(row[0]).strip(),
        bundle_id=str(row[1]).strip(),
        target_hypothesis_id=str(row[2]).strip(),
        decision=str(row[3]).strip(),
        final_title=str(row[4] or "").strip(),
        target_record_id=str(row[5] or "").strip(),
        confidence=float(row[6] or 0.0),
        reason=str(row[7] or "").strip(),
        evidence_summary=EvidenceSummary.model_validate(_load_json_text(row[8], default={})),
        assessment=ResearchAssessment.model_validate(_load_json_text(row[9], default={})),
        record=record_payload if record_payload else None,
    )


def get_video_context_cache(
    conn: duckdb.DuckDBPyConnection,
    bvid: str,
) -> dict | None:
    """按 BVID 获取视频上下文缓存。"""
    row = conn.execute(
        """
        SELECT
            bvid,
            video_url,
            title,
            status,
            duration_seconds,
            summary,
            description_text,
            content_text,
            transcript_excerpt,
            chapters_json,
            raw_payload_json,
            skip_reason,
            updated_at
        FROM video_context_cache
        WHERE bvid = ?
        """,
        [bvid],
    ).fetchone()
    if not row:
        return None

    return {
        "bvid": row[0],
        "video_url": row[1],
        "title": row[2],
        "status": row[3],
        "duration_seconds": row[4],
        "summary": row[5],
        "description_text": row[6],
        "content_text": row[7],
        "transcript_excerpt": row[8],
        "chapters": _load_json_text(row[9], default=[]),
        "raw_payload": _load_json_text(row[10], default={}),
        "skip_reason": row[11],
        "updated_at": row[12].isoformat() if row[12] else None,
    }


def upsert_video_context_cache(
    conn: duckdb.DuckDBPyConnection,
    *,
    bvid: str,
    video_url: str,
    title: str,
    status: str,
    duration_seconds: int | None,
    summary: str = "",
    description_text: str = "",
    content_text: str = "",
    transcript_excerpt: str = "",
    chapters: list[dict] | None = None,
    raw_payload: dict | None = None,
    skip_reason: str = "",
) -> None:
    """写入视频上下文缓存。"""
    conn.execute(
        """
        INSERT INTO video_context_cache (
            bvid,
            video_url,
            title,
            status,
            duration_seconds,
            summary,
            description_text,
            content_text,
            transcript_excerpt,
            chapters_json,
            raw_payload_json,
            skip_reason,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (bvid) DO UPDATE
        SET video_url = excluded.video_url,
            title = excluded.title,
            status = excluded.status,
            duration_seconds = excluded.duration_seconds,
            summary = excluded.summary,
            description_text = excluded.description_text,
            content_text = excluded.content_text,
            transcript_excerpt = excluded.transcript_excerpt,
            chapters_json = excluded.chapters_json,
            raw_payload_json = excluded.raw_payload_json,
            skip_reason = excluded.skip_reason,
            updated_at = excluded.updated_at
        """,
        [
            bvid,
            video_url,
            title,
            status,
            duration_seconds,
            summary,
            description_text,
            content_text,
            transcript_excerpt,
            json.dumps(chapters or [], ensure_ascii=False),
            json.dumps(raw_payload or {}, ensure_ascii=False),
            skip_reason,
            datetime.now(),
        ],
    )


def _load_json_text(value, *, default):
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
