"""
评论证据包主轴下的核心数据模型。
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from meme_detector.researcher.models import MemeRecord


class InsightStatus(StrEnum):
    pending = "pending"
    inspected = "inspected"
    bundled = "bundled"
    researched = "researched"


class SpanType(StrEnum):
    template_core = "template_core"
    quote_core = "quote_core"
    slot_filler = "slot_filler"
    entity_work = "entity_work"
    entity_person = "entity_person"
    entity_event = "entity_event"
    context_term = "context_term"
    unknown = "unknown"


class QueryPriority(StrEnum):
    high = "high"
    medium = "medium"
    low = "low"


class HypothesisType(StrEnum):
    template_meme = "template_meme"
    quote_meme = "quote_meme"
    entity_is_meme = "entity_is_meme"
    entity_only = "entity_only"
    mixed_expression = "mixed_expression"
    unclear = "unclear"


class HypothesisStatus(StrEnum):
    pending = "pending"
    evidenced = "evidenced"
    queued = "queued"
    accepted = "accepted"
    rejected = "rejected"
    manual_review = "manual_review"
    superseded = "superseded"
    merged = "merged"


class HypothesisSpanRole(StrEnum):
    primary = "primary"
    related = "related"
    slot_filler = "slot_filler"
    counter_example = "counter_example"


class QueryMode(StrEnum):
    literal = "literal"
    contextual = "contextual"
    origin_probe = "origin_probe"
    meme_probe = "meme_probe"


class SourceKind(StrEnum):
    video_summary = "video_summary"
    video_transcript = "video_transcript"
    web_search_summary = "web_search_summary"
    web_search_result = "web_search_result"


class EvidenceDirection(StrEnum):
    supports_meme = "supports_meme"
    supports_template = "supports_template"
    supports_origin = "supports_origin"
    supports_entity_only = "supports_entity_only"
    contradicts_meme = "contradicts_meme"
    context_only = "context_only"
    unclear = "unclear"


class SuggestedAction(StrEnum):
    search_then_review = "search_then_review"
    search_optional = "search_optional"
    direct_review = "direct_review"
    discard_low_value = "discard_low_value"


class ResearchDecisionType(StrEnum):
    accept = "accept"
    reject = "reject"
    rewrite_title = "rewrite_title"
    manual_review = "manual_review"
    merge_into_existing = "merge_into_existing"


class VideoRef(BaseModel):
    bvid: str = Field(min_length=1)
    title: str = ""
    url: str = ""
    partition: str = ""
    collected_date: date | None = None


class Insight(BaseModel):
    insight_id: str
    bvid: str
    collected_date: date
    comment_text: str
    worth_investigating: bool
    signal_score: float = Field(ge=0.0, le=1.0)
    reason: str
    status: InsightStatus = InsightStatus.pending


class Span(BaseModel):
    span_id: str
    insight_id: str
    raw_text: str = Field(min_length=1)
    normalized_text: str = Field(min_length=1)
    span_type: SpanType
    char_start: int | None = Field(default=None, ge=0)
    char_end: int | None = Field(default=None, ge=0)
    confidence: float = Field(ge=0.0, le=1.0)
    is_primary: bool = False
    query_priority: QueryPriority = QueryPriority.low
    reason: str

    @model_validator(mode="after")
    def validate_char_range(self) -> Span:
        if self.char_start is not None and self.char_end is not None and self.char_end < self.char_start:
            raise ValueError("char_end must be greater than or equal to char_start")
        return self


class Hypothesis(BaseModel):
    hypothesis_id: str
    insight_id: str
    candidate_title: str = Field(min_length=1)
    hypothesis_type: HypothesisType
    miner_opinion: str
    support_score: float = Field(ge=0.0, le=1.0)
    counter_score: float = Field(ge=0.0, le=1.0)
    uncertainty_score: float = Field(ge=0.0, le=1.0)
    suggested_action: SuggestedAction = SuggestedAction.search_optional
    status: HypothesisStatus = HypothesisStatus.pending


class HypothesisSpanLink(BaseModel):
    hypothesis_id: str
    span_id: str
    role: HypothesisSpanRole


class Evidence(BaseModel):
    evidence_id: str
    hypothesis_id: str
    span_id: str | None = None
    query: str = Field(min_length=1)
    query_mode: QueryMode
    source_kind: SourceKind
    source_title: str = ""
    source_url: str = ""
    snippet: str = ""
    evidence_direction: EvidenceDirection
    evidence_strength: float = Field(ge=0.0, le=1.0)


class MinerSummary(BaseModel):
    recommended_hypothesis_id: str | None = None
    should_queue_for_research: bool
    reason: str


class MinerBundle(BaseModel):
    bundle_id: str
    insight: Insight
    video_refs: list[VideoRef] = Field(default_factory=list)
    spans: list[Span] = Field(default_factory=list)
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    hypothesis_spans: list[HypothesisSpanLink] = Field(default_factory=list)
    evidences: list[Evidence] = Field(default_factory=list)
    miner_summary: MinerSummary

    @model_validator(mode="after")
    def validate_bundle(self) -> MinerBundle:
        if not self.hypotheses:
            raise ValueError("bundle must contain at least one hypothesis")

        span_ids = {item.span_id for item in self.spans}
        hypothesis_ids = {item.hypothesis_id for item in self.hypotheses}
        primary_links: dict[str, int] = {item.hypothesis_id: 0 for item in self.hypotheses}

        for span in self.spans:
            if span.insight_id != self.insight.insight_id:
                raise ValueError("all spans must belong to the bundle insight")

        for hypothesis in self.hypotheses:
            if hypothesis.insight_id != self.insight.insight_id:
                raise ValueError("all hypotheses must belong to the bundle insight")

        for link in self.hypothesis_spans:
            if link.hypothesis_id not in hypothesis_ids:
                raise ValueError("hypothesis_spans contains an unknown hypothesis_id")
            if link.span_id not in span_ids:
                raise ValueError("hypothesis_spans contains an unknown span_id")
            if link.role == HypothesisSpanRole.primary:
                primary_links[link.hypothesis_id] += 1

        missing_primary = [item for item, count in primary_links.items() if count == 0]
        if missing_primary:
            raise ValueError(f"hypotheses missing primary span link: {', '.join(sorted(missing_primary))}")

        if self.miner_summary.recommended_hypothesis_id and (
            self.miner_summary.recommended_hypothesis_id not in hypothesis_ids
        ):
            raise ValueError("recommended_hypothesis_id must reference an existing hypothesis")

        for evidence in self.evidences:
            if evidence.hypothesis_id not in hypothesis_ids:
                raise ValueError("evidence references an unknown hypothesis_id")
            if evidence.span_id is not None and evidence.span_id not in span_ids:
                raise ValueError("evidence references an unknown span_id")

        return self


class EvidenceSummary(BaseModel):
    support_count: int = 0
    counter_count: int = 0
    unclear_count: int = 0


class ResearchAssessment(BaseModel):
    is_core_meme_unit: bool
    is_reusable_expression: bool
    is_entity_reference_only: bool
    needs_human_review: bool
    competing_hypothesis_exists: bool


class ResearchDecision(BaseModel):
    decision_id: str
    bundle_id: str
    target_hypothesis_id: str
    decision: ResearchDecisionType
    final_title: str = ""
    target_record_id: str = ""
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    evidence_summary: EvidenceSummary = Field(default_factory=EvidenceSummary)
    assessment: ResearchAssessment
    record: MemeRecord | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> ResearchDecision:
        if self.decision in {ResearchDecisionType.accept, ResearchDecisionType.rewrite_title}:
            if self.record is None:
                raise ValueError("record is required for accept and rewrite_title decisions")
            if not self.final_title:
                raise ValueError("final_title is required for accept and rewrite_title decisions")
        else:
            if self.record is not None:
                raise ValueError("record must be empty for non-accepting decisions")
        return self


CategoryLiteral = Literal["抽象", "谐音", "游戏", "影视", "音乐", "社会现象", "二次元", "其他"]
