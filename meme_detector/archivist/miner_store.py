"""
Miner 领域 DuckDB 读写。
"""

from __future__ import annotations

import json
from datetime import datetime

import duckdb

from meme_detector.archivist.sql_utils import build_where_clause, count_rows, make_in_placeholders
from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)


def upsert_miner_comment_insights(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    if not insights:
        return

    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        bvid = str(item.get("bvid", "")).strip()
        collected_date = item.get("collected_date")
        comment_text = str(item.get("comment_text", "")).strip()
        if not insight_id or not bvid or not collected_date or not comment_text:
            continue
        tags = item.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        status = str(item.get("status", "")).strip()
        if not status:
            is_high_value = float(
                item.get("confidence", 0.0) or 0.0
            ) >= settings.miner_comment_confidence_threshold and (
                bool(item.get("is_meme_candidate")) or bool(item.get("is_insider_knowledge"))
            )
            status = "pending_bundle" if is_high_value else "discarded"
        rows.append(
            (
                insight_id,
                bvid,
                collected_date,
                str(item.get("partition", "")).strip(),
                str(item.get("title", "")).strip(),
                str(item.get("description", "")).strip(),
                str(item.get("video_url", "")).strip(),
                json.dumps([str(tag).strip() for tag in tags if str(tag).strip()], ensure_ascii=False),
                comment_text,
                float(item.get("confidence", 0.0) or 0.0),
                bool(item.get("is_meme_candidate")),
                bool(item.get("is_insider_knowledge")),
                str(item.get("reason", "")).strip(),
                json.dumps(item.get("video_context") or {}, ensure_ascii=False),
                status,
                now,
                now,
            )
        )

    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO miner_comment_insights (
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (insight_id) DO UPDATE
        SET partition = excluded.partition,
            title = excluded.title,
            description = excluded.description,
            video_url = excluded.video_url,
            tags_json = excluded.tags_json,
            comment_text = excluded.comment_text,
            confidence = excluded.confidence,
            is_meme_candidate = excluded.is_meme_candidate,
            is_insider_knowledge = excluded.is_insider_knowledge,
            reason = excluded.reason,
            video_context_json = excluded.video_context_json,
            updated_at = excluded.updated_at,
            status = CASE
                WHEN miner_comment_insights.status = 'bundled' THEN miner_comment_insights.status
                WHEN miner_comment_insights.status = 'bundling' THEN miner_comment_insights.status
                ELSE excluded.status
            END
        """,
        rows,
    )


def get_pending_miner_comment_insights(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = 200,
) -> list[dict]:
    query = """
        SELECT
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at
        FROM miner_comment_insights
        WHERE status IN ('pending_bundle', 'bundle_failed')
        ORDER BY confidence DESC, collected_date ASC, bvid ASC
    """
    params: list[int] = []
    if limit is not None:
        query += "\n        LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [
        {
            "insight_id": row[0],
            "bvid": row[1],
            "collected_date": row[2],
            "partition": row[3],
            "title": row[4],
            "description": row[5],
            "url": row[6],
            "tags": _load_json_text(row[7], default=[]),
            "comment_text": row[8],
            "confidence": row[9],
            "is_meme_candidate": row[10],
            "is_insider_knowledge": row[11],
            "reason": row[12],
            "video_context": _load_json_text(row[13], default={}),
            "status": row[14],
            "created_at": row[15],
            "updated_at": row[16],
        }
        for row in rows
    ]


def get_miner_comment_insights_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    keyword: str | None = None,
    bvid: str | None = None,
    only_meme_candidates: bool = False,
    only_insider_knowledge: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    where_parts: list[str] = []
    params: list[str | int | bool] = []

    if status:
        where_parts.append("status = ?")
        params.append(status)
    if keyword:
        where_parts.append("(title LIKE ? OR description LIKE ? OR comment_text LIKE ? OR reason LIKE ?)")
        keyword_like = f"%{keyword}%"
        params.extend([keyword_like, keyword_like, keyword_like, keyword_like])
    if bvid:
        where_parts.append("bvid LIKE ?")
        params.append(f"%{bvid}%")
    if only_meme_candidates:
        where_parts.append("is_meme_candidate = TRUE")
    if only_insider_knowledge:
        where_parts.append("is_insider_knowledge = TRUE")

    where_clause = build_where_clause(where_parts)
    total = count_rows(
        conn,
        from_clause="miner_comment_insights",
        where_clause=where_clause,
        params=params,
    )

    rows = conn.execute(
        f"""
        SELECT
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at,
            (
                SELECT bundle_id
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_id,
            (
                SELECT status
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_status
        FROM miner_comment_insights
        {where_clause}
        ORDER BY collected_date DESC, confidence DESC, updated_at DESC, bvid ASC
        LIMIT ?
        OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()

    return {
        "items": [_serialize_miner_comment_insight(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_miner_comment_insight(
    conn: duckdb.DuckDBPyConnection,
    insight_id: str,
) -> dict | None:
    row = conn.execute(
        """
        SELECT
            insight_id,
            bvid,
            collected_date,
            partition,
            title,
            description,
            video_url,
            tags_json,
            comment_text,
            confidence,
            is_meme_candidate,
            is_insider_knowledge,
            reason,
            video_context_json,
            status,
            created_at,
            updated_at,
            (
                SELECT bundle_id
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_id,
            (
                SELECT status
                FROM comment_insights bundles
                WHERE bundles.insight_id = miner_comment_insights.insight_id
            ) AS bundle_status
        FROM miner_comment_insights
        WHERE insight_id = ?
        """,
        [insight_id],
    ).fetchone()
    if not row:
        return None
    return _serialize_miner_comment_insight(row)


def mark_miner_comment_insights_processed(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        if insight_id:
            rows.append((now, insight_id))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE miner_comment_insights
        SET status = 'bundled',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def mark_miner_comment_insights_bundling(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        if insight_id:
            rows.append((now, insight_id))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE miner_comment_insights
        SET status = 'bundling',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def mark_miner_comment_insights_bundle_failed(
    conn: duckdb.DuckDBPyConnection,
    insights: list[dict],
) -> None:
    rows = []
    now = datetime.now()
    for item in insights:
        insight_id = str(item.get("insight_id", "")).strip()
        if insight_id:
            rows.append((now, insight_id))
    if not rows:
        return
    conn.executemany(
        """
        UPDATE miner_comment_insights
        SET status = 'bundle_failed',
            updated_at = ?
        WHERE insight_id = ?
        """,
        rows,
    )


def upsert_comment_bundle(
    conn: duckdb.DuckDBPyConnection,
    bundle,
) -> None:
    from meme_detector.pipeline_models import MinerBundle

    if not isinstance(bundle, MinerBundle):
        bundle = MinerBundle.model_validate(bundle)

    now = datetime.now()
    insight = bundle.insight
    conn.execute(
        """
        INSERT INTO comment_insights (
            bundle_id,
            insight_id,
            bvid,
            collected_date,
            comment_text,
            worth_investigating,
            signal_score,
            reason,
            status,
            video_refs_json,
            miner_summary_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (bundle_id) DO UPDATE
        SET insight_id = excluded.insight_id,
            bvid = excluded.bvid,
            collected_date = excluded.collected_date,
            comment_text = excluded.comment_text,
            worth_investigating = excluded.worth_investigating,
            signal_score = excluded.signal_score,
            reason = excluded.reason,
            status = excluded.status,
            video_refs_json = excluded.video_refs_json,
            miner_summary_json = excluded.miner_summary_json,
            updated_at = excluded.updated_at
        """,
        [
            bundle.bundle_id,
            insight.insight_id,
            insight.bvid,
            insight.collected_date,
            insight.comment_text,
            insight.worth_investigating,
            insight.signal_score,
            insight.reason,
            insight.status.value,
            json.dumps([item.model_dump(mode="json") for item in bundle.video_refs], ensure_ascii=False),
            json.dumps(bundle.miner_summary.model_dump(mode="json"), ensure_ascii=False),
            now,
            now,
        ],
    )

    existing_hypothesis_rows = conn.execute(
        """
        SELECT hypothesis_id
        FROM hypotheses
        WHERE bundle_id = ?
        """,
        [bundle.bundle_id],
    ).fetchall()
    existing_hypothesis_ids = [str(row[0]).strip() for row in existing_hypothesis_rows if str(row[0]).strip()]
    if existing_hypothesis_ids:
        placeholders = make_in_placeholders(existing_hypothesis_ids)
        conn.execute(
            f"DELETE FROM evidences WHERE hypothesis_id IN ({placeholders})",
            existing_hypothesis_ids,
        )
        conn.execute(
            f"DELETE FROM hypothesis_spans WHERE hypothesis_id IN ({placeholders})",
            existing_hypothesis_ids,
        )
    conn.execute("DELETE FROM hypotheses WHERE bundle_id = ?", [bundle.bundle_id])
    conn.execute("DELETE FROM comment_spans WHERE insight_id = ?", [insight.insight_id])

    if bundle.spans:
        conn.executemany(
            """
            INSERT INTO comment_spans (
                span_id,
                insight_id,
                raw_text,
                normalized_text,
                span_type,
                char_start,
                char_end,
                confidence,
                is_primary,
                query_priority,
                reason,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.span_id,
                    item.insight_id,
                    item.raw_text,
                    item.normalized_text,
                    item.span_type.value,
                    item.char_start,
                    item.char_end,
                    item.confidence,
                    item.is_primary,
                    item.query_priority.value,
                    item.reason,
                    now,
                    now,
                )
                for item in bundle.spans
            ],
        )

    if bundle.hypotheses:
        conn.executemany(
            """
            INSERT INTO hypotheses (
                hypothesis_id,
                bundle_id,
                insight_id,
                candidate_title,
                hypothesis_type,
                miner_opinion,
                support_score,
                counter_score,
                uncertainty_score,
                suggested_action,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.hypothesis_id,
                    bundle.bundle_id,
                    item.insight_id,
                    item.candidate_title,
                    item.hypothesis_type.value,
                    item.miner_opinion,
                    item.support_score,
                    item.counter_score,
                    item.uncertainty_score,
                    item.suggested_action.value,
                    item.status.value,
                    now,
                    now,
                )
                for item in bundle.hypotheses
            ],
        )

    if bundle.hypothesis_spans:
        seen_hypothesis_spans: set[tuple[str, str]] = set()
        conn.executemany(
            """
            INSERT INTO hypothesis_spans (
                hypothesis_id,
                span_id,
                role
            )
            VALUES (?, ?, ?)
            """,
            [
                (
                    item.hypothesis_id,
                    item.span_id,
                    item.role.value,
                )
                for item in bundle.hypothesis_spans
                if not (
                    (item.hypothesis_id, item.span_id) in seen_hypothesis_spans
                    or seen_hypothesis_spans.add((item.hypothesis_id, item.span_id))
                )
            ],
        )

    if bundle.evidences:
        conn.executemany(
            """
            INSERT INTO evidences (
                evidence_id,
                hypothesis_id,
                span_id,
                query,
                query_mode,
                source_kind,
                source_title,
                source_url,
                snippet,
                evidence_direction,
                evidence_strength,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.evidence_id,
                    item.hypothesis_id,
                    item.span_id,
                    item.query,
                    item.query_mode.value,
                    item.source_kind.value,
                    item.source_title,
                    item.source_url,
                    item.snippet,
                    item.evidence_direction.value,
                    item.evidence_strength,
                    now,
                )
                for item in bundle.evidences
            ],
        )


def get_comment_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    bundle_id: str | None = None,
    insight_id: str | None = None,
):
    from meme_detector.pipeline_models import (
        Evidence,
        Hypothesis,
        HypothesisSpanLink,
        Insight,
        MinerBundle,
        MinerSummary,
        Span,
        VideoRef,
    )

    if not bundle_id and not insight_id:
        raise ValueError("bundle_id or insight_id is required")

    if bundle_id:
        row = conn.execute(
            """
            SELECT
                bundle_id,
                insight_id,
                bvid,
                collected_date,
                comment_text,
                worth_investigating,
                signal_score,
                reason,
                status,
                video_refs_json,
                miner_summary_json
            FROM comment_insights
            WHERE bundle_id = ?
            """,
            [bundle_id],
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT
                bundle_id,
                insight_id,
                bvid,
                collected_date,
                comment_text,
                worth_investigating,
                signal_score,
                reason,
                status,
                video_refs_json,
                miner_summary_json
            FROM comment_insights
            WHERE insight_id = ?
            """,
            [insight_id],
        ).fetchone()

    if not row:
        return None

    resolved_bundle_id = str(row[0]).strip()
    resolved_insight_id = str(row[1]).strip()
    insight = Insight(
        insight_id=resolved_insight_id,
        bvid=str(row[2]).strip(),
        collected_date=row[3],
        comment_text=str(row[4]).strip(),
        worth_investigating=bool(row[5]),
        signal_score=float(row[6] or 0.0),
        reason=str(row[7] or "").strip(),
        status=str(row[8] or "pending"),
    )
    video_refs = [
        VideoRef.model_validate(item) for item in _load_json_text(row[9], default=[]) if isinstance(item, dict)
    ]
    miner_summary = MinerSummary.model_validate(_load_json_text(row[10], default={}))

    span_rows = conn.execute(
        """
        SELECT
            span_id,
            insight_id,
            raw_text,
            normalized_text,
            span_type,
            char_start,
            char_end,
            confidence,
            is_primary,
            query_priority,
            reason
        FROM comment_spans
        WHERE insight_id = ?
        ORDER BY is_primary DESC, confidence DESC, span_id ASC
        """,
        [resolved_insight_id],
    ).fetchall()
    spans = [
        Span(
            span_id=str(item[0]).strip(),
            insight_id=str(item[1]).strip(),
            raw_text=str(item[2]).strip(),
            normalized_text=str(item[3]).strip(),
            span_type=str(item[4]).strip(),
            char_start=item[5],
            char_end=item[6],
            confidence=float(item[7] or 0.0),
            is_primary=bool(item[8]),
            query_priority=str(item[9] or "low"),
            reason=str(item[10] or "").strip(),
        )
        for item in span_rows
    ]

    hypothesis_rows = conn.execute(
        """
        SELECT
            hypothesis_id,
            insight_id,
            candidate_title,
            hypothesis_type,
            miner_opinion,
            support_score,
            counter_score,
            uncertainty_score,
            suggested_action,
            status
        FROM hypotheses
        WHERE bundle_id = ?
        ORDER BY support_score DESC, counter_score ASC, hypothesis_id ASC
        """,
        [resolved_bundle_id],
    ).fetchall()
    hypotheses = [
        Hypothesis(
            hypothesis_id=str(item[0]).strip(),
            insight_id=str(item[1]).strip(),
            candidate_title=str(item[2]).strip(),
            hypothesis_type=str(item[3]).strip(),
            miner_opinion=str(item[4] or "").strip(),
            support_score=float(item[5] or 0.0),
            counter_score=float(item[6] or 0.0),
            uncertainty_score=float(item[7] or 0.0),
            suggested_action=str(item[8] or "search_optional"),
            status=str(item[9] or "pending"),
        )
        for item in hypothesis_rows
    ]

    if hypotheses:
        hypothesis_ids = [item.hypothesis_id for item in hypotheses]
        placeholders = ", ".join("?" for _ in hypothesis_ids)
        link_rows = conn.execute(
            f"""
            SELECT
                hypothesis_id,
                span_id,
                role
            FROM hypothesis_spans
            WHERE hypothesis_id IN ({placeholders})
            ORDER BY hypothesis_id ASC, span_id ASC
            """,
            hypothesis_ids,
        ).fetchall()
        evidence_rows = conn.execute(
            f"""
            SELECT
                evidence_id,
                hypothesis_id,
                span_id,
                query,
                query_mode,
                source_kind,
                source_title,
                source_url,
                snippet,
                evidence_direction,
                evidence_strength
            FROM evidences
            WHERE hypothesis_id IN ({placeholders})
            ORDER BY hypothesis_id ASC, evidence_strength DESC, evidence_id ASC
            """,
            hypothesis_ids,
        ).fetchall()
    else:
        link_rows = []
        evidence_rows = []

    hypothesis_spans = [
        HypothesisSpanLink(
            hypothesis_id=str(item[0]).strip(),
            span_id=str(item[1]).strip(),
            role=str(item[2]).strip(),
        )
        for item in link_rows
    ]
    if hypotheses and spans:
        primary_hypothesis_ids = {item.hypothesis_id for item in hypothesis_spans if item.role.value == "primary"}
        linked_span_ids_by_hypothesis: dict[str, list[str]] = {}
        for item in hypothesis_spans:
            linked_span_ids_by_hypothesis.setdefault(item.hypothesis_id, []).append(item.span_id)

        primary_span_ids = [item.span_id for item in spans if item.is_primary]
        fallback_default_span_id = primary_span_ids[0] if primary_span_ids else spans[0].span_id
        missing_primary_hypothesis_ids: list[str] = []

        for hypothesis in hypotheses:
            if hypothesis.hypothesis_id in primary_hypothesis_ids:
                continue
            fallback_span_id = linked_span_ids_by_hypothesis.get(hypothesis.hypothesis_id, [fallback_default_span_id])[
                0
            ]
            hypothesis_spans.append(
                HypothesisSpanLink(
                    hypothesis_id=hypothesis.hypothesis_id,
                    span_id=fallback_span_id,
                    role="primary",
                )
            )
            missing_primary_hypothesis_ids.append(hypothesis.hypothesis_id)

        if missing_primary_hypothesis_ids:
            logger.warning(
                "comment bundle missing primary span links; reconstructed on read",
                extra={
                    "event": "comment_bundle_primary_span_reconstructed",
                    "bundle_id": resolved_bundle_id,
                    "hypothesis_ids": missing_primary_hypothesis_ids,
                },
            )
    evidences = [
        Evidence(
            evidence_id=str(item[0]).strip(),
            hypothesis_id=str(item[1]).strip(),
            span_id=str(item[2]).strip() or None,
            query=str(item[3]).strip(),
            query_mode=str(item[4]).strip(),
            source_kind=str(item[5]).strip(),
            source_title=str(item[6] or "").strip(),
            source_url=str(item[7] or "").strip(),
            snippet=str(item[8] or "").strip(),
            evidence_direction=str(item[9]).strip(),
            evidence_strength=float(item[10] or 0.0),
        )
        for item in evidence_rows
    ]

    return MinerBundle(
        bundle_id=resolved_bundle_id,
        insight=insight,
        video_refs=video_refs,
        spans=spans,
        hypotheses=hypotheses,
        hypothesis_spans=hypothesis_spans,
        evidences=evidences,
        miner_summary=miner_summary,
    )


def list_queued_comment_bundles(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = 50,
) -> list[dict]:
    query = """
        SELECT
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.signal_score,
            ci.status,
            COUNT(h.hypothesis_id) AS hypothesis_count
        FROM comment_insights ci
        JOIN hypotheses h ON h.bundle_id = ci.bundle_id
        WHERE ci.status = 'bundled'
          AND h.status IN ('queued', 'evidenced')
        GROUP BY
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.signal_score,
            ci.status
        ORDER BY ci.collected_date ASC, ci.signal_score DESC, ci.bundle_id ASC
    """
    params: list[int] = []
    if limit is not None:
        query += "\n        LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [
        {
            "bundle_id": row[0],
            "insight_id": row[1],
            "bvid": row[2],
            "collected_date": row[3],
            "comment_text": row[4],
            "signal_score": float(row[5] or 0.0),
            "status": row[6],
            "hypothesis_count": int(row[7] or 0),
        }
        for row in rows
    ]


def get_comment_bundles_page(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    queued_only: bool = False,
    keyword: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    where_parts: list[str] = []
    params: list[str | int] = []
    if status:
        where_parts.append("ci.status = ?")
        params.append(status)
    if queued_only:
        where_parts.append(
            "EXISTS (SELECT 1 FROM hypotheses hq WHERE hq.bundle_id = ci.bundle_id AND hq.status = 'queued')"
        )
    if keyword:
        where_parts.append("(ci.comment_text LIKE ? OR ci.reason LIKE ? OR ci.bvid LIKE ?)")
        keyword_like = f"%{keyword}%"
        params.extend([keyword_like, keyword_like, keyword_like])

    where_clause = build_where_clause(where_parts)
    total = count_rows(
        conn,
        from_clause="comment_insights ci",
        where_clause=where_clause,
        params=params,
    )

    rows = conn.execute(
        f"""
        SELECT
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.worth_investigating,
            ci.signal_score,
            ci.reason,
            ci.status,
            ci.video_refs_json,
            ci.miner_summary_json,
            COUNT(h.hypothesis_id) AS hypothesis_count,
            COUNT(h.hypothesis_id) FILTER (WHERE h.status = 'queued') AS queued_hypothesis_count,
            COUNT(h.hypothesis_id) FILTER (WHERE h.status = 'accepted') AS accepted_hypothesis_count,
            COUNT(e.evidence_id) AS evidence_count,
            MAX(rd.decision) AS latest_decision
        FROM comment_insights ci
        LEFT JOIN hypotheses h ON h.bundle_id = ci.bundle_id
        LEFT JOIN evidences e ON e.hypothesis_id = h.hypothesis_id
        LEFT JOIN research_decisions rd ON rd.bundle_id = ci.bundle_id
        {where_clause}
        GROUP BY
            ci.bundle_id,
            ci.insight_id,
            ci.bvid,
            ci.collected_date,
            ci.comment_text,
            ci.worth_investigating,
            ci.signal_score,
            ci.reason,
            ci.status,
            ci.video_refs_json,
            ci.miner_summary_json
        ORDER BY ci.collected_date DESC, ci.signal_score DESC, ci.bundle_id ASC
        LIMIT ?
        OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()

    items = []
    for row in rows:
        miner_summary = _load_json_text(row[10], default={})
        if not isinstance(miner_summary, dict):
            miner_summary = {}
        video_refs = _load_json_text(row[9], default=[])
        if not isinstance(video_refs, list):
            video_refs = []
        items.append(
            {
                "bundle_id": row[0],
                "insight_id": row[1],
                "bvid": row[2],
                "collected_date": row[3],
                "comment_text": row[4],
                "worth_investigating": bool(row[5]),
                "signal_score": float(row[6] or 0.0),
                "reason": row[7],
                "status": row[8],
                "video_refs": video_refs,
                "recommended_hypothesis_id": miner_summary.get("recommended_hypothesis_id"),
                "miner_summary_reason": miner_summary.get("reason", ""),
                "hypothesis_count": int(row[11] or 0),
                "queued_hypothesis_count": int(row[12] or 0),
                "accepted_hypothesis_count": int(row[13] or 0),
                "evidence_count": int(row[14] or 0),
                "latest_decision": row[15] or "",
            }
        )

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def get_comment_bundle_detail(
    conn: duckdb.DuckDBPyConnection,
    bundle_id: str,
) -> dict | None:
    bundle = get_comment_bundle(conn, bundle_id=bundle_id)
    if bundle is None:
        return None

    decision_rows = conn.execute(
        """
        SELECT
            decision_id,
            decision,
            final_title,
            target_record_id,
            confidence,
            reason,
            created_at
        FROM research_decisions
        WHERE bundle_id = ?
        ORDER BY created_at DESC, decision_id DESC
        """,
        [bundle_id],
    ).fetchall()

    decisions = [
        {
            "decision_id": row[0],
            "decision": row[1],
            "final_title": row[2],
            "target_record_id": row[3],
            "confidence": float(row[4] or 0.0),
            "reason": row[5],
            "created_at": row[6],
        }
        for row in decision_rows
    ]
    return {
        "bundle": bundle.model_dump(mode="json"),
        "decisions": decisions,
    }


def _serialize_miner_comment_insight(row: tuple) -> dict:
    tags = _load_json_text(row[7], default=[])
    if not isinstance(tags, list):
        tags = []

    video_context = _load_json_text(row[13], default={})
    if not isinstance(video_context, dict):
        video_context = {}

    return {
        "insight_id": row[0],
        "bvid": row[1],
        "collected_date": row[2],
        "partition": row[3],
        "title": row[4],
        "description": row[5],
        "video_url": row[6],
        "url": row[6],
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "comment_text": row[8],
        "confidence": row[9],
        "is_meme_candidate": row[10],
        "is_insider_knowledge": row[11],
        "reason": row[12],
        "video_context": video_context,
        "status": row[14],
        "created_at": row[15],
        "updated_at": row[16],
        "bundle_id": row[17] or "",
        "bundle_status": row[18] or "",
    }


def _load_json_text(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default
