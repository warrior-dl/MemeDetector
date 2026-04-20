"""
Research 新主流程：基于评论证据包裁决 hypothesis。
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from openai import AsyncOpenAI
from pydantic import BaseModel, Field

from meme_detector.agent_tracing import TraceTimelineBuilder
from meme_detector.config import settings
from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion_detailed,
    resolve_llm_config,
)
from meme_detector.llm_factory import (
    request_json_chat_completion as _request_json_chat_completion,
)
from meme_detector.logging_utils import get_logger
from meme_detector.pipeline_models import (
    EvidenceSummary,
    MinerBundle,
    ResearchAssessment,
    ResearchDecision,
)
from meme_detector.researcher.models import MemeRecord
from meme_detector.researcher.taxonomy import (
    normalize_category as _normalize_category_from_taxonomy,
)
from meme_detector.researcher.taxonomy import (
    normalize_lifecycle_stage as _normalize_lifecycle_stage_from_taxonomy,
)

logger = get_logger(__name__)

_DECISION_SYSTEM = """\
你是一位专业的互联网亚文化研究员。
你会收到 Miner 产出的评论证据包，其中包含：
- 评论原文
- 多个 span
- 多个 competing hypotheses
- 视频上下文
- 联网搜索证据（正证据和反证据）

你的任务：
1. 判断哪个 hypothesis 才是真正的传播核心
2. 决定 accept / reject / rewrite_title / manual_review / merge_into_existing
3. 只有在 accept 或 rewrite_title 时，才生成最终词条 record

重要要求：
1. 不要默认实体名、作品名、人名就是梗
2. 当 evidence 主要显示“它只是作品名/实体名”时，应倾向 reject
3. 当真正传播单位不是当前标题而是另一个模板句/引用句时，应使用 rewrite_title
4. 输出单个 JSON 对象，不要输出 Markdown

输出字段：
{
  "decision": "accept | reject | rewrite_title | manual_review | merge_into_existing",
  "target_hypothesis_index": 0,
  "final_title": "",
  "target_record_id": "",
  "confidence": 0.0,
  "reason": "一句话说明裁决原因",
  "evidence_summary": {
    "support_count": 0,
    "counter_count": 0,
    "unclear_count": 0
  },
  "assessment": {
    "is_core_meme_unit": true,
    "is_reusable_expression": true,
    "is_entity_reference_only": false,
    "needs_human_review": false,
    "competing_hypothesis_exists": true
  },
  "record": null
}

record 仅在 decision 为 accept 或 rewrite_title 时提供，字段要求：
- id
- title
- alias
- definition
- origin
- category
- platform
- heat_index
- lifecycle_stage
- source_urls
- confidence_score

严格格式要求：
- `alias` 必须是 JSON 字符串数组，例如 `["别名1", "别名2"]`
- `category` 必须是 JSON 字符串数组，只能从以下值中选择：`抽象`、`谐音`、`游戏`、`影视`、`音乐`、`社会现象`、`二次元`、`其他`
- `lifecycle_stage` 只能输出以下英文枚举之一：`emerging`、`peak`、`declining`
- `source_urls` 必须是 JSON 字符串数组
- 不要输出中文生命周期，如“增长期/高峰期/衰退期”
- 不要输出用斜杠拼接的 category 字符串，如“谐音梗/鬼畜梗”

合法 record 示例：
{
  "id": "闭嘴，如果你惹怒了我……",
  "title": "闭嘴，如果你惹怒了我……",
  "alias": ["闭嘴，如果你惹怒了我"],
  "definition": "一种模仿放狠话的模板句式。",
  "origin": "常见于二创改写和表情包语境。",
  "category": ["二次元", "其他"],
  "platform": "Bilibili",
  "heat_index": 66,
  "lifecycle_stage": "emerging",
  "source_urls": ["https://example.com/source"],
  "confidence_score": 0.86
}
"""


class _DraftEvidenceSummary(BaseModel):
    support_count: int = 0
    counter_count: int = 0
    unclear_count: int = 0


class _DraftAssessment(BaseModel):
    is_core_meme_unit: bool
    is_reusable_expression: bool
    is_entity_reference_only: bool
    needs_human_review: bool
    competing_hypothesis_exists: bool


class _DraftDecision(BaseModel):
    decision: str
    target_hypothesis_index: int = Field(ge=0)
    final_title: str = ""
    target_record_id: str = ""
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    evidence_summary: _DraftEvidenceSummary = Field(default_factory=_DraftEvidenceSummary)
    assessment: _DraftAssessment
    record: dict | None = None


async def request_json_chat_completion(
    *,
    client: AsyncOpenAI,
    model_name: str,
    messages: list[dict[str, str]],
) -> str:
    return await _request_json_chat_completion(
        client=client,
        model_name=model_name,
        messages=messages,
    )


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return []


def _split_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        parts = re.split(r"[\/|、，,；;]+", value)
        return [part.strip() for part in parts if part.strip()]
    return []


def _normalize_source_urls(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        parts = re.split(r"[\s,，；;|]+", value.strip())
        return [part.strip() for part in parts if part.strip()]
    return []


def _normalize_category(value: Any) -> list[str]:
    return _normalize_category_from_taxonomy(value)


def _normalize_lifecycle_stage(value: Any) -> str:
    return _normalize_lifecycle_stage_from_taxonomy(value)


def _normalize_record_payload(record: dict[str, Any], *, target_title: str, today: date) -> dict[str, Any]:
    payload = dict(record)
    payload["id"] = payload.get("id") or target_title
    payload["title"] = payload.get("title") or target_title
    payload["alias"] = _coerce_list(payload.get("alias"))
    payload["category"] = _normalize_category(payload.get("category"))
    payload["source_urls"] = _normalize_source_urls(payload.get("source_urls"))
    payload["lifecycle_stage"] = _normalize_lifecycle_stage(payload.get("lifecycle_stage"))
    payload["platform"] = str(payload.get("platform") or "Bilibili").strip() or "Bilibili"
    payload["first_detected_at"] = payload.get("first_detected_at") or today
    payload["updated_at"] = payload.get("updated_at") or today
    return payload


async def decide_bundle(
    bundle: MinerBundle,
    *,
    today: date | None = None,
    trace: TraceTimelineBuilder | None = None,
) -> ResearchDecision:
    config = resolve_llm_config("research")
    if not config.api_key.strip():
        raise RuntimeError("RESEARCH_LLM_API_KEY/LLM_API_KEY 未配置，无法执行 Research 裁决")

    client = build_async_openai_client(
        "research",
        timeout=settings.research_llm_timeout_seconds,
        max_retries=settings.research_llm_max_retries,
        client_cls=AsyncOpenAI,
    )
    payload = _build_bundle_payload(bundle)
    messages = [
        {"role": "system", "content": _DECISION_SYSTEM},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    llm_response = await _request_decision_completion(
        client=client,
        model_name=config.model,
        messages=messages,
    )
    raw = llm_response["content"]
    if trace is not None:
        trace.add_llm_usage(llm_response.get("usage"))
    data = load_json_response(raw)
    draft = _DraftDecision.model_validate(data)
    if trace is not None:
        trace.add_step(
            event_type="llm_generation",
            stage="reason",
            title="裁决 competing hypotheses",
            status="success",
            summary=f"模型返回决策：{draft.decision}",
            input_data={
                "bundle_id": bundle.bundle_id,
                "hypothesis_count": len(bundle.hypotheses),
                "evidence_count": len(bundle.evidences),
            },
            output_data={
                "decision": draft.decision,
                "final_title": draft.final_title,
                "reason": draft.reason,
                "raw": raw[:2000],
            },
            metadata={
                "model": config.model,
                "provider": config.provider,
                "usage": llm_response.get("usage", {}),
            },
        )
    target_index = min(draft.target_hypothesis_index, len(bundle.hypotheses) - 1)
    target_hypothesis = bundle.hypotheses[target_index]
    target_title = draft.final_title or target_hypothesis.candidate_title
    current_date = today or date.today()
    record = None
    if draft.record:
        record = MemeRecord.model_validate(
            _normalize_record_payload(
                draft.record,
                target_title=target_title,
                today=current_date,
            )
        )

    return ResearchDecision(
        decision_id=f"decision_{bundle.bundle_id}",
        bundle_id=bundle.bundle_id,
        target_hypothesis_id=target_hypothesis.hypothesis_id,
        decision=draft.decision,
        final_title=target_title if draft.decision in {"accept", "rewrite_title"} else draft.final_title,
        target_record_id=draft.target_record_id or (record.id if record else ""),
        confidence=draft.confidence,
        reason=draft.reason,
        evidence_summary=EvidenceSummary.model_validate(draft.evidence_summary.model_dump()),
        assessment=ResearchAssessment.model_validate(draft.assessment.model_dump()),
        record=record,
    )


async def _request_decision_completion(
    *,
    client: AsyncOpenAI,
    model_name: str,
    messages: list[dict[str, str]],
) -> dict[str, Any]:
    if request_json_chat_completion is _request_json_chat_completion:
        return await request_json_chat_completion_detailed(
            client=client,
            model_name=model_name,
            messages=messages,
        )
    return {
        "content": await request_json_chat_completion(
            client=client,
            model_name=model_name,
            messages=messages,
        ),
        "usage": {},
    }


def _build_bundle_payload(bundle: MinerBundle) -> dict:
    return {
        "bundle_id": bundle.bundle_id,
        "insight": bundle.insight.model_dump(mode="json"),
        "video_refs": [item.model_dump(mode="json") for item in bundle.video_refs],
        "spans": [item.model_dump(mode="json") for item in bundle.spans],
        "hypotheses": [item.model_dump(mode="json") for item in bundle.hypotheses],
        "hypothesis_spans": [item.model_dump(mode="json") for item in bundle.hypothesis_spans],
        "evidences": [item.model_dump(mode="json") for item in bundle.evidences],
        "miner_summary": bundle.miner_summary.model_dump(mode="json"),
    }
