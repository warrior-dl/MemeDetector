"""
Miner 第二阶段：将高价值评论整理为评论证据包。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from time import perf_counter

from openai import AsyncOpenAI
from pydantic import BaseModel, Field

from meme_detector.config import settings
from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion,
    resolve_llm_config,
)
from meme_detector.logging_utils import get_logger
from meme_detector.pipeline_models import (
    Evidence,
    EvidenceDirection,
    Hypothesis,
    HypothesisSpanLink,
    Insight,
    InsightStatus,
    MinerBundle,
    MinerSummary,
    QueryMode,
    QueryPriority,
    SourceKind,
    Span,
    SpanType,
    SuggestedAction,
    VideoRef,
)
from meme_detector.researcher.tools import volcengine_web_search, volcengine_web_search_summary

logger = get_logger(__name__)
_MAX_BUNDLE_SEARCH_QUERIES = 2
_BUNDLE_SEARCH_RESULT_COUNT = 1

_PLAN_SYSTEM = """\
你是一位中文互联网亚文化侦查员。
你的任务是先拆解评论中的可疑传播片段，再规划中性的联网搜索查询。

要求：
1. 不要下最终“是不是梗”的结论
2. 优先识别可复用模板句、引用句、填槽项、作品名、人名、事件名、背景词
3. 搜索默认中性，不要默认加“梗”
4. 只有在确有必要时，才把查询模式标成 meme_probe
5. 如果只是大模型通常已知的常识、圈层旧知识、作品基础设定、老台词背景，不要机械搜索
6. 能靠评论文本、视频上下文和常识判断的，就不要额外搜索

输出 JSON：
{
  "span_candidates": [
    {
      "text": "片段原文",
      "span_type": "template_core | quote_core | slot_filler | entity_work | entity_person | entity_event | context_term | unknown",
      "query_priority": "high | medium | low",
      "reason": "一句话说明"
    }
  ],
  "search_queries": [
    {
      "query": "查询词",
      "query_mode": "literal | contextual | origin_probe | meme_probe",
      "span_text": "关联片段，可为空",
      "reason": "一句话说明"
    }
  ],
  "miner_note": "这条评论里最值得继续侦查的方向"
}

限制：
- 最多返回 4 个 span_candidates
- 最多返回 2 个 search_queries
- query_mode 优先使用 literal 和 contextual
- 如果无需联网搜索，可以返回空数组
"""

_SYNTHESIS_SYSTEM = """\
你是一位中文互联网亚文化侦查员。
你会收到一条高价值评论、视频上下文、初步拆分的候选片段，以及系统已经替你执行好的联网搜索结果。

你的任务是：
1. 识别这条评论中最可能的传播核心对象
2. 比较多个 competing hypotheses
3. 产出结构化评论证据包，供后续 Research 裁决

重要要求：
1. 不要输出 definition、origin、category 等最终词条字段
2. 不要预设某个实体名一定是梗
3. 如果某个片段更像作品名/人名/事件名，要把它作为反证或 slot_filler 处理
4. hypothesis 至少一个，每个 hypothesis 至少需要一个 primary span

输出 JSON：
{
  "spans": [
    {
      "text": "片段原文",
      "span_type": "template_core | quote_core | slot_filler | entity_work | entity_person | entity_event | context_term | unknown",
      "confidence": 0.0,
      "is_primary": true,
      "query_priority": "high | medium | low",
      "reason": "一句话说明"
    }
  ],
  "hypotheses": [
    {
      "title": "暂定假设标题",
      "hypothesis_type": "template_meme | quote_meme | entity_is_meme | entity_only | mixed_expression | unclear",
      "miner_opinion": "Miner 的证据倾向",
      "support_score": 0.0,
      "counter_score": 0.0,
      "uncertainty_score": 0.0,
      "suggested_action": "search_then_review | search_optional | direct_review | discard_low_value"
    }
  ],
  "hypothesis_spans": [
    {
      "hypothesis_index": 0,
      "span_index": 0,
      "role": "primary | related | slot_filler | counter_example"
    }
  ],
  "evidences": [
    {
      "hypothesis_index": 0,
      "span_index": 0,
      "query": "查询词",
      "query_mode": "literal | contextual | origin_probe | meme_probe",
      "source_kind": "video_summary | video_transcript | web_search_summary | web_search_result",
      "source_title": "来源标题",
      "source_url": "来源 URL，可为空",
      "snippet": "摘要，可为空",
      "evidence_direction": "supports_meme | supports_template | supports_origin | supports_entity_only | contradicts_meme | context_only | unclear",
      "evidence_strength": 0.0
    }
  ],
  "recommended_hypothesis_index": 0,
  "should_queue_for_research": true,
  "reason": "一句话总结当前证据倾向"
}
"""


class _PlannedSpan(BaseModel):
    text: str
    span_type: str
    query_priority: str = "low"
    reason: str = ""


class _SearchQuery(BaseModel):
    query: str
    query_mode: str = "literal"
    span_text: str = ""
    reason: str = ""


class _BundlePlan(BaseModel):
    span_candidates: list[_PlannedSpan] = Field(default_factory=list)
    search_queries: list[_SearchQuery] = Field(default_factory=list)
    miner_note: str = ""


class _SynthesizedSpan(BaseModel):
    text: str
    span_type: str
    confidence: float = Field(ge=0.0, le=1.0)
    is_primary: bool = False
    query_priority: str = "low"
    reason: str = ""


class _SynthesizedHypothesis(BaseModel):
    title: str
    hypothesis_type: str
    miner_opinion: str = ""
    support_score: float = Field(ge=0.0, le=1.0)
    counter_score: float = Field(ge=0.0, le=1.0)
    uncertainty_score: float = Field(ge=0.0, le=1.0)
    suggested_action: str = "search_optional"


class _SynthesizedLink(BaseModel):
    hypothesis_index: int = Field(ge=0)
    span_index: int = Field(ge=0)
    role: str = "related"


class _SynthesizedEvidence(BaseModel):
    hypothesis_index: int = Field(ge=0)
    span_index: int | None = Field(default=None, ge=0)
    query: str
    query_mode: str
    source_kind: str
    source_title: str = ""
    source_url: str = ""
    snippet: str = ""
    evidence_direction: str
    evidence_strength: float = Field(ge=0.0, le=1.0)


class _BundleSynthesis(BaseModel):
    spans: list[_SynthesizedSpan] = Field(default_factory=list)
    hypotheses: list[_SynthesizedHypothesis] = Field(default_factory=list)
    hypothesis_spans: list[_SynthesizedLink] = Field(default_factory=list)
    evidences: list[_SynthesizedEvidence] = Field(default_factory=list)
    recommended_hypothesis_index: int | None = Field(default=None, ge=0)
    should_queue_for_research: bool = False
    reason: str = ""


@dataclass
class _SearchEvidencePack:
    query: str
    query_mode: str
    span_text: str
    reason: str
    summary_result: dict
    web_results: list[dict]


def _is_high_value_insight(item: dict) -> bool:
    return (
        float(item.get("confidence", 0.0) or 0.0) >= settings.miner_comment_confidence_threshold
        and (bool(item.get("is_meme_candidate")) or bool(item.get("is_insider_knowledge")))
    )


async def build_bundles_from_insights(video: dict, insights: list[dict]) -> list[MinerBundle]:
    high_value_insights = [item for item in insights if _is_high_value_insight(item)]
    if not high_value_insights:
        return []
    llm_config = resolve_llm_config("miner")
    if not llm_config.api_key.strip():
        logger.info(
            "miner bundle generation skipped because miner llm api key is missing",
            extra={
                "event": "miner_bundle_generation_skipped_missing_llm_key",
                "bvid": str(video.get("bvid", "")).strip() or "UNKNOWN",
            },
        )
        return []
    if not settings.web_search_api_key.strip():
        logger.info(
            "miner bundle generation skipped because web search api key is missing",
            extra={
                "event": "miner_bundle_generation_skipped_missing_web_search_key",
                "bvid": str(video.get("bvid", "")).strip() or "UNKNOWN",
            },
        )
        return []

    bundles: list[MinerBundle] = []
    for item in high_value_insights:
        bundles.append(await build_comment_bundle(video, item))
    return bundles


async def build_comment_bundle(video: dict, insight_item: dict) -> MinerBundle:
    insight_id = str(insight_item.get("insight_id", "")).strip() or "UNKNOWN"
    bvid = str(video.get("bvid", "")).strip() or "UNKNOWN"
    client = build_async_openai_client(
        "miner",
        timeout=settings.miner_llm_timeout_seconds,
        max_retries=settings.miner_llm_max_retries,
        client_cls=AsyncOpenAI,
    )
    llm_config = resolve_llm_config("miner")
    started_at = perf_counter()
    plan = await _plan_comment_bundle(client, llm_config.model, video, insight_item)
    plan_finished_at = perf_counter()
    logger.info(
        "miner bundle plan ready",
        extra={
            "event": "miner_bundle_plan_ready",
            "insight_id": insight_id,
            "bvid": bvid,
            "duration_ms": round((plan_finished_at - started_at) * 1000, 2),
            "planned_span_count": len(plan.span_candidates),
            "planned_query_count": len(plan.search_queries),
        },
    )
    search_packs = await _collect_search_evidence(plan.search_queries)
    search_finished_at = perf_counter()
    logger.info(
        "miner bundle search evidence collected",
        extra={
            "event": "miner_bundle_search_evidence_collected",
            "insight_id": insight_id,
            "bvid": bvid,
            "duration_ms": round((search_finished_at - plan_finished_at) * 1000, 2),
            "search_query_count": len(search_packs),
            "web_result_count": sum(len(item.web_results) for item in search_packs),
        },
    )
    synthesis = await _synthesize_comment_bundle(
        client=client,
        model_name=llm_config.model,
        video=video,
        insight_item=insight_item,
        plan=plan,
        search_packs=search_packs,
    )
    synthesis_finished_at = perf_counter()
    logger.info(
        "miner bundle synthesis ready",
        extra={
            "event": "miner_bundle_synthesis_ready",
            "insight_id": insight_id,
            "bvid": bvid,
            "duration_ms": round((synthesis_finished_at - search_finished_at) * 1000, 2),
            "span_count": len(synthesis.spans),
            "hypothesis_count": len(synthesis.hypotheses),
            "evidence_count": len(synthesis.evidences),
        },
    )
    bundle = _materialize_bundle(video, insight_item, synthesis)
    logger.info(
        "miner bundle materialized",
        extra={
            "event": "miner_bundle_materialized",
            "insight_id": insight_id,
            "bvid": bvid,
            "duration_ms": round((perf_counter() - started_at) * 1000, 2),
            "span_count": len(bundle.spans),
            "hypothesis_count": len(bundle.hypotheses),
            "evidence_count": len(bundle.evidences),
        },
    )
    return bundle


async def _plan_comment_bundle(
    client: AsyncOpenAI,
    model_name: str,
    video: dict,
    insight_item: dict,
) -> _BundlePlan:
    context = insight_item.get("video_context", {}) or {}
    prompt = "\n".join(
        [
            f"BVID: {str(video.get('bvid', '')).strip()}",
            f"标题: {str(video.get('title', '')).strip()}",
            f"分区: {str(video.get('partition', '')).strip()}",
            f"评论: {str(insight_item.get('comment_text', '')).strip()}",
            f"Miner 初判置信度: {float(insight_item.get('confidence', 0.0) or 0.0):.2f}",
            f"Miner 初判理由: {str(insight_item.get('reason', '')).strip()}",
            f"视频摘要: {str(context.get('summary', '')).strip()[:500] or '无'}",
            f"字幕摘录: {str(context.get('transcript_excerpt', '')).strip()[:500] or '无'}",
        ]
    )
    raw = await request_json_chat_completion(
        client=client,
        model_name=model_name,
        messages=[
            {"role": "system", "content": _PLAN_SYSTEM},
            {"role": "user", "content": prompt},
        ],
    )
    data = load_json_response(raw)
    return _BundlePlan.model_validate(data)


async def _collect_search_evidence(search_queries: list[_SearchQuery]) -> list[_SearchEvidencePack]:
    packs: list[_SearchEvidencePack] = []
    seen_queries: set[tuple[str, str]] = set()

    for item in search_queries[:_MAX_BUNDLE_SEARCH_QUERIES]:
        query = str(item.query).strip()
        query_mode = str(item.query_mode or "literal").strip() or "literal"
        if not query:
            continue
        dedup_key = (query, query_mode)
        if dedup_key in seen_queries:
            continue
        seen_queries.add(dedup_key)
        summary_result = await volcengine_web_search_summary(
            query,
            num_results=_BUNDLE_SEARCH_RESULT_COUNT,
        )
        web_results: list[dict] = []
        if not _is_summary_sufficient(summary_result):
            web_results = await volcengine_web_search(
                query,
                num_results=_BUNDLE_SEARCH_RESULT_COUNT,
            )
        packs.append(
            _SearchEvidencePack(
                query=query,
                query_mode=query_mode,
                span_text=str(item.span_text or "").strip(),
                reason=str(item.reason or "").strip(),
                summary_result=summary_result if isinstance(summary_result, dict) else {"error": "invalid summary result"},
                web_results=web_results if isinstance(web_results, list) else [],
            )
        )

    return packs


def _is_summary_sufficient(summary_result: dict) -> bool:
    if not isinstance(summary_result, dict) or "error" in summary_result:
        return False
    summary_text = str(summary_result.get("summary", "")).strip()
    results = summary_result.get("results", [])
    if len(summary_text) >= 80:
        return True
    if not isinstance(results, list):
        return False
    rich_results = 0
    for item in results:
        if not isinstance(item, dict):
            continue
        if len(str(item.get("content", "")).strip()) >= 80 or len(str(item.get("snippet", "")).strip()) >= 50:
            rich_results += 1
    return rich_results >= 2 or (len(summary_text) >= 40 and rich_results >= 1)


async def _synthesize_comment_bundle(
    *,
    client: AsyncOpenAI,
    model_name: str,
    video: dict,
    insight_item: dict,
    plan: _BundlePlan,
    search_packs: list[_SearchEvidencePack],
) -> _BundleSynthesis:
    context = insight_item.get("video_context", {}) or {}
    payload = {
        "video": {
            "bvid": str(video.get("bvid", "")).strip(),
            "title": str(video.get("title", "")).strip(),
            "partition": str(video.get("partition", "")).strip(),
        },
        "comment": str(insight_item.get("comment_text", "")).strip(),
        "miner_signal": {
            "confidence": float(insight_item.get("confidence", 0.0) or 0.0),
            "reason": str(insight_item.get("reason", "")).strip(),
            "is_meme_candidate": bool(insight_item.get("is_meme_candidate")),
            "is_insider_knowledge": bool(insight_item.get("is_insider_knowledge")),
        },
        "video_context": {
            "status": str(context.get("status", "")).strip(),
            "summary": str(context.get("summary", "")).strip()[:800],
            "transcript_excerpt": str(context.get("transcript_excerpt", "")).strip()[:800],
        },
        "planned_spans": [item.model_dump(mode="json") for item in plan.span_candidates],
        "search_evidence": [
            {
                "query": item.query,
                "query_mode": item.query_mode,
                "span_text": item.span_text,
                "reason": item.reason,
                "summary_result": item.summary_result,
                "web_results": item.web_results,
            }
            for item in search_packs
        ],
    }
    raw = await request_json_chat_completion(
        client=client,
        model_name=model_name,
        messages=[
            {"role": "system", "content": _SYNTHESIS_SYSTEM},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
    )
    data = load_json_response(raw)
    return _BundleSynthesis.model_validate(data)


def _materialize_bundle(video: dict, insight_item: dict, synthesis: _BundleSynthesis) -> MinerBundle:
    insight_id = str(insight_item.get("insight_id", "")).strip()
    bundle_id = "bundle_" + hashlib.sha256(insight_id.encode("utf-8")).hexdigest()[:16]
    comment_text = str(insight_item.get("comment_text", "")).strip()
    spans: list[Span] = []
    for index, item in enumerate(synthesis.spans):
        span_id = _build_hash_id("span", insight_id, str(index), item.text)
        char_start, char_end = _locate_span(comment_text, item.text, index)
        spans.append(
            Span(
                span_id=span_id,
                insight_id=insight_id,
                raw_text=item.text,
                normalized_text=_normalize_text(item.text),
                span_type=item.span_type,
                char_start=char_start,
                char_end=char_end,
                confidence=item.confidence,
                is_primary=item.is_primary,
                query_priority=item.query_priority,
                reason=item.reason,
            )
        )

    span_id_by_index = {index: item.span_id for index, item in enumerate(spans)}
    hypotheses: list[Hypothesis] = []
    hypothesis_id_by_index: dict[int, str] = {}
    for index, item in enumerate(synthesis.hypotheses):
        hypothesis_id = _build_hash_id("hyp", insight_id, str(index), item.title)
        hypothesis_id_by_index[index] = hypothesis_id
        hypotheses.append(
            Hypothesis(
                hypothesis_id=hypothesis_id,
                insight_id=insight_id,
                candidate_title=item.title,
                hypothesis_type=item.hypothesis_type,
                miner_opinion=item.miner_opinion,
                support_score=item.support_score,
                counter_score=item.counter_score,
                uncertainty_score=item.uncertainty_score,
                suggested_action=item.suggested_action,
                status="queued" if synthesis.should_queue_for_research else "evidenced",
            )
        )

    hypothesis_spans: list[HypothesisSpanLink] = []
    for item in synthesis.hypothesis_spans:
        hypothesis_id = hypothesis_id_by_index.get(item.hypothesis_index)
        span_id = span_id_by_index.get(item.span_index)
        if not hypothesis_id or not span_id:
            continue
        hypothesis_spans.append(
            HypothesisSpanLink(
                hypothesis_id=hypothesis_id,
                span_id=span_id,
                role=item.role,
            )
        )

    primary_linked = {item.hypothesis_id for item in hypothesis_spans if item.role.value == "primary"}
    for index, hypothesis in enumerate(hypotheses):
        if hypothesis.hypothesis_id in primary_linked:
            continue
        fallback_span_id = _choose_primary_span_for_hypothesis(spans)
        if fallback_span_id:
            hypothesis_spans.append(
                HypothesisSpanLink(
                    hypothesis_id=hypothesis.hypothesis_id,
                    span_id=fallback_span_id,
                    role="primary",
                )
            )

    hypothesis_spans = _dedupe_hypothesis_spans(hypothesis_spans)

    evidences: list[Evidence] = []
    for index, item in enumerate(synthesis.evidences):
        hypothesis_id = hypothesis_id_by_index.get(item.hypothesis_index)
        if not hypothesis_id:
            continue
        query = str(item.query or "").strip()
        if not query:
            logger.warning(
                "miner bundle evidence skipped because query is empty",
                extra={
                    "event": "miner_bundle_evidence_skipped_empty_query",
                    "insight_id": insight_id,
                    "hypothesis_index": item.hypothesis_index,
                    "span_index": item.span_index,
                    "source_kind": item.source_kind,
                    "source_title": str(item.source_title or "").strip(),
                },
            )
            continue
        evidences.append(
            Evidence(
                evidence_id=_build_hash_id("ev", insight_id, str(index), query, item.source_title),
                hypothesis_id=hypothesis_id,
                span_id=span_id_by_index.get(item.span_index) if item.span_index is not None else None,
                query=query,
                query_mode=item.query_mode,
                source_kind=item.source_kind,
                source_title=str(item.source_title or "").strip(),
                source_url=str(item.source_url or "").strip(),
                snippet=str(item.snippet or "").strip(),
                evidence_direction=item.evidence_direction,
                evidence_strength=item.evidence_strength,
            )
        )

    insight = Insight(
        insight_id=insight_id,
        bvid=str(insight_item.get("bvid", "")).strip(),
        collected_date=insight_item.get("collected_date"),
        comment_text=comment_text,
        worth_investigating=True,
        signal_score=float(insight_item.get("confidence", 0.0) or 0.0),
        reason=str(insight_item.get("reason", "")).strip(),
        status=InsightStatus.bundled,
    )
    video_refs = [
        VideoRef(
            bvid=str(video.get("bvid", "")).strip(),
            title=str(video.get("title", "")).strip(),
            url=str(video.get("url", "")).strip(),
            partition=str(video.get("partition", "")).strip(),
            collected_date=video.get("collected_date"),
        )
    ]

    recommended_hypothesis_id = None
    if synthesis.recommended_hypothesis_index is not None:
        recommended_hypothesis_id = hypothesis_id_by_index.get(synthesis.recommended_hypothesis_index)
    if recommended_hypothesis_id is None and hypotheses:
        recommended_hypothesis_id = hypotheses[0].hypothesis_id

    return MinerBundle(
        bundle_id=bundle_id,
        insight=insight,
        video_refs=video_refs,
        spans=spans,
        hypotheses=hypotheses,
        hypothesis_spans=hypothesis_spans,
        evidences=evidences,
        miner_summary=MinerSummary(
            recommended_hypothesis_id=recommended_hypothesis_id,
            should_queue_for_research=synthesis.should_queue_for_research,
            reason=synthesis.reason or "Miner 已完成证据整理",
        ),
    )


def _build_hash_id(prefix: str, *parts: str) -> str:
    joined = "|".join(str(item or "").strip() for item in parts)
    return f"{prefix}_{hashlib.sha256(joined.encode('utf-8')).hexdigest()[:16]}"


def _dedupe_hypothesis_spans(items: list[HypothesisSpanLink]) -> list[HypothesisSpanLink]:
    deduped: list[HypothesisSpanLink] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        key = (item.hypothesis_id, item.span_id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _normalize_text(value: str) -> str:
    return "".join(str(value or "").split())


def _locate_span(comment_text: str, span_text: str, fallback_index: int) -> tuple[int | None, int | None]:
    if not span_text:
        return None, None
    index = comment_text.find(span_text)
    if index >= 0:
        return index, index + len(span_text)
    normalized_comment = _normalize_text(comment_text)
    normalized_span = _normalize_text(span_text)
    normalized_index = normalized_comment.find(normalized_span)
    if normalized_index >= 0:
        return normalized_index, normalized_index + len(normalized_span)
    return fallback_index, fallback_index + len(span_text)


def _choose_primary_span_for_hypothesis(spans: list[Span]) -> str | None:
    if not spans:
        return None
    ordered = sorted(
        spans,
        key=lambda item: (item.is_primary, item.confidence, -len(item.raw_text)),
        reverse=True,
    )
    return ordered[0].span_id
