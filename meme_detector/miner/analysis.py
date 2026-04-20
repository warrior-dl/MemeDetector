"""
Miner 评论分析与对话落库。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from contextlib import closing
from datetime import datetime

from openai import AsyncOpenAI

from meme_detector.agent_tracing import TraceTimelineBuilder, start_langfuse_trace
from meme_detector.archivist.agent_store import (
    create_agent_conversation,
    finish_agent_conversation,
    replace_agent_trace_events,
)
from meme_detector.archivist.schema import get_conn
from meme_detector.config import settings
from meme_detector.llm_factory import (
    build_async_openai_client,
    load_json_response,
    request_json_chat_completion_detailed,
    resolve_llm_config,
)
from meme_detector.logging_utils import get_logger
from meme_detector.miner.models import CommentInsightResult
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.run_tracker import get_current_run_id

logger = get_logger(__name__)

_MINER_SYSTEM = """\
你是一位中文互联网亚文化观察员。
你的任务不是直接下最终结论，而是根据视频上下文和评论内容，判断“这条评论是否值得进入后续 Research 深挖”。

判断目标：
1. is_meme_candidate：评论里是否可能包含梗、梗线索、稳定复用表达、谐音梗、抽象表达
2. is_insider_knowledge：评论里是否可能包含圈层知识、黑话、约定俗成的文化背景、特定作品/人物/事件的圈内引用

输出要求：
- 对每条评论都给出结果
- confidence 为 0 到 1
- reason 用一句话解释
- 保守判断，普通夸赞、通用情绪词、灌水回复不要高分

返回 JSON：
{
  "results": [
    {
      "index": 0,
      "is_meme_candidate": true,
      "is_insider_knowledge": false,
      "confidence": 0.82,
      "reason": "评论包含稳定复用的抽象表达，像潜在梗。"
    }
  ]
}
"""


async def score_video_comments(
    video: dict,
    comments: list[str],
    *,
    client_cls: type[AsyncOpenAI] = AsyncOpenAI,
    video_context_loader: Callable[[str], object] = get_bilibili_video_context,
    run_id_getter: Callable[[], str | None] = get_current_run_id,
) -> list[dict]:
    conversation_id = create_miner_conversation(video, run_id_getter=run_id_getter)
    conversation_messages: list[dict] = []
    bvid = str(video.get("bvid", "")).strip()
    llm_config = resolve_llm_config("miner")
    trace = TraceTimelineBuilder(
        conversation_id=conversation_id or "",
        run_id=run_id_getter() or "",
        agent_name="miner",
        entity_type="video",
        entity_id=bvid,
    )
    with start_langfuse_trace(
        name=f"miner:{bvid or 'unknown-video'}",
        session_id=run_id_getter(),
        metadata={
            "agent_name": "miner",
            "conversation_id": conversation_id or "",
            "bvid": bvid,
            "model": llm_config.model,
            "provider": llm_config.provider,
        },
    ) as langfuse_trace:
        context_started_at = datetime.now()
        context = await video_context_loader(bvid)
        trace.add_step(
            event_type="input",
            stage="prepare",
            title="加载视频上下文",
            status="success",
            summary=f"上下文状态：{str(context.get('status', '')).strip() or 'unknown'}",
            input_data={"bvid": bvid},
            output_data={"status": context.get("status", ""), "title": video.get("title", "")},
            started_at=context_started_at,
            finished_at=datetime.now(),
        )
        client = build_async_openai_client(
            "miner",
            timeout=settings.miner_llm_timeout_seconds,
            max_retries=settings.miner_llm_max_retries,
            client_cls=client_cls,
        )
        chunks = [
            comments[i : i + settings.miner_comments_batch_size]
            for i in range(0, len(comments), settings.miner_comments_batch_size)
        ]
        trace.add_step(
            event_type="input",
            stage="prepare",
            title="切分评论批次",
            status="success",
            summary=f"共 {len(comments)} 条评论，切分为 {len(chunks)} 个批次",
            output_data={"comment_count": len(comments), "chunk_count": len(chunks)},
        )
        logger.info(
            "miner video analysis prepared",
            extra={
                "event": "miner_video_analysis_prepared",
                "bvid": bvid,
                "comment_count": len(comments),
                "chunk_count": len(chunks),
                "model_name": llm_config.model,
                "provider": llm_config.provider,
                "status": context.get("status", ""),
                "conversation_id": conversation_id,
            },
        )
        all_results: list[dict] = []
        try:
            for chunk_index, chunk in enumerate(chunks):
                offset = chunk_index * settings.miner_comments_batch_size
                fallback_reason = "模型未返回有效结果"
                user_msg = build_miner_prompt(video, context, chunk)
                chunk_started_at = datetime.now()
                logger.info(
                    "miner comment chunk started",
                    extra={
                        "event": "miner_chunk_started",
                        "bvid": bvid,
                        "chunk_index": chunk_index,
                        "comment_count": len(chunk),
                    },
                )
                conversation_messages.extend(
                    [
                        {
                            "role": "system",
                            "chunk_index": chunk_index,
                            "content": truncate_text(_MINER_SYSTEM, 3000),
                        },
                        {
                            "role": "user",
                            "chunk_index": chunk_index,
                            "content": truncate_text(user_msg, 12000),
                        },
                    ]
                )
                try:
                    llm_response = await request_chunk_comment_scores(
                        client=client,
                        user_msg=user_msg,
                        model_name=llm_config.model,
                    )
                    raw = llm_response["content"]
                    trace.add_llm_usage(llm_response.get("usage"))
                    conversation_messages.append(
                        {
                            "role": "assistant",
                            "chunk_index": chunk_index,
                            "content": truncate_text(raw, 12000),
                        }
                    )
                    items = extract_chunk_items(raw)
                    logger.info(
                        "miner comment chunk response received",
                        extra={
                            "event": "miner_chunk_response_received",
                            "bvid": bvid,
                            "chunk_index": chunk_index,
                            "result_count": len(items),
                        },
                    )
                    trace.add_step(
                        event_type="llm_generation",
                        stage="reason",
                        title=f"评论批次判定 #{chunk_index + 1}",
                        status="success",
                        summary=f"返回 {len(items)} 条结构化结果",
                        input_data={
                            "chunk_index": chunk_index,
                            "comment_count": len(chunk),
                            "messages": conversation_messages[-3:],
                        },
                        output_data={
                            "raw": truncate_text(raw, 2000),
                            "result_count": len(items),
                        },
                        metadata={
                            "model": llm_config.model,
                            "provider": llm_config.provider,
                            "usage": llm_response.get("usage", {}),
                        },
                        started_at=chunk_started_at,
                        finished_at=datetime.now(),
                    )
                except Exception as exc:
                    fallback_reason = format_chunk_failure_reason(exc)
                    logger.warning(
                        "miner comment chunk analysis failed",
                        extra={
                            "event": "miner_chunk_failed",
                            "bvid": bvid,
                            "chunk_index": chunk_index,
                        },
                        exc_info=exc,
                    )
                    conversation_messages.append(
                        {
                            "role": "assistant",
                            "chunk_index": chunk_index,
                            "error": summarize_exception(exc),
                        }
                    )
                    trace.add_step(
                        event_type="llm_generation",
                        stage="reason",
                        title=f"评论批次判定 #{chunk_index + 1}",
                        status="failed",
                        summary=summarize_exception(exc),
                        input_data={
                            "chunk_index": chunk_index,
                            "comment_count": len(chunk),
                        },
                        output_data={"error": summarize_exception(exc)},
                        metadata={"model": llm_config.model, "provider": llm_config.provider},
                        started_at=chunk_started_at,
                        finished_at=datetime.now(),
                    )
                    items = []

                parsed_by_index: dict[int, CommentInsightResult] = {}
                for item in items:
                    try:
                        parsed = CommentInsightResult(**item)
                    except Exception:
                        continue
                    parsed_by_index[parsed.index] = parsed

                for local_index, comment_text in enumerate(chunk):
                    parsed = parsed_by_index.get(local_index)
                    if parsed is None:
                        parsed = CommentInsightResult(
                            index=local_index,
                            is_meme_candidate=False,
                            is_insider_knowledge=False,
                            confidence=0.0,
                            reason=fallback_reason,
                        )
                    all_results.append(
                        materialize_insight_record(
                            video=video,
                            context=context,
                            comment_text=comment_text,
                            parsed=parsed,
                            global_index=offset + local_index,
                        )
                    )
                chunk_results = all_results[offset : offset + len(chunk)]
                chunk_high_value_count = sum(
                    1
                    for item in chunk_results
                    if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
                    and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
                )
                logger.info(
                    "miner comment chunk completed",
                    extra={
                        "event": "miner_chunk_completed",
                        "bvid": bvid,
                        "chunk_index": chunk_index,
                        "comment_count": len(chunk_results),
                        "high_value_count": chunk_high_value_count,
                    },
                )
        except Exception as exc:
            logger.exception(
                "miner video analysis failed",
                extra={
                    "event": "miner_video_analysis_failed",
                    "bvid": bvid,
                    "conversation_id": conversation_id,
                },
            )
            trace.add_step(
                event_type="error",
                stage="finalize",
                title="视频评论初筛失败",
                status="failed",
                summary=str(exc),
                output_data={"error": str(exc)},
            )
            persist_miner_conversation(
                conversation_id=conversation_id,
                status="failed",
                video=video,
                comments=comments,
                results=all_results,
                conversation_messages=conversation_messages,
                trace=trace,
                langfuse_trace_id=langfuse_trace.trace_id,
                langfuse_public_url=langfuse_trace.trace_url,
                error_message=str(exc),
            )
            raise

        trace.add_step(
            event_type="persist",
            stage="finalize",
            title="写入评论线索结果",
            status="success",
            summary=f"共写入 {len(all_results)} 条结果",
            output_data={"result_count": len(all_results)},
        )
        persist_miner_conversation(
            conversation_id=conversation_id,
            status="success",
            video=video,
            comments=comments,
            results=all_results,
            conversation_messages=conversation_messages,
            trace=trace,
            langfuse_trace_id=langfuse_trace.trace_id,
            langfuse_public_url=langfuse_trace.trace_url,
        )
        logger.info(
            "miner video analysis completed",
            extra={
                "event": "miner_video_analysis_completed",
                "bvid": bvid,
                "result_count": len(all_results),
                "high_value_count": sum(
                    1
                    for item in all_results
                    if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
                    and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
                ),
                "conversation_id": conversation_id,
            },
        )
        return all_results


async def request_chunk_comment_scores(
    *,
    client: AsyncOpenAI,
    user_msg: str,
    model_name: str,
) -> dict:
    return await request_json_chat_completion_detailed(
        client=client,
        model_name=model_name,
        messages=[
            {"role": "system", "content": _MINER_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
    )


def extract_chunk_items(raw: str) -> list[dict]:
    data = load_json_response(raw)
    items = data if isinstance(data, list) else data.get("results", [])
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def format_chunk_failure_reason(exc: Exception) -> str:
    summary = summarize_exception(exc)
    return f"模型请求失败: {summary}"[:120]


def summarize_exception(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        return exc.__class__.__name__
    return message[:80]


def create_miner_conversation(
    video: dict,
    *,
    run_id_getter: Callable[[], str | None] = get_current_run_id,
) -> str | None:
    run_id = run_id_getter()
    if not run_id:
        return None
    with closing(get_conn()) as conn:
        return create_agent_conversation(
            conn,
            run_id=run_id,
            agent_name="miner",
            word=str(video.get("bvid", "")).strip() or str(video.get("title", "")).strip() or "UNKNOWN",
            entity_type="video",
            entity_id=str(video.get("bvid", "")).strip(),
            langfuse_session_id=run_id,
        )


def persist_miner_conversation(
    *,
    conversation_id: str | None,
    status: str,
    video: dict,
    comments: list[str],
    results: list[dict],
    conversation_messages: list[dict],
    trace: TraceTimelineBuilder,
    langfuse_trace_id: str = "",
    langfuse_public_url: str = "",
    error_message: str = "",
) -> None:
    if not conversation_id:
        return
    high_value_count = sum(
        1
        for item in results
        if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
        and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
    )
    summary = (
        f"{str(video.get('bvid', '')).strip() or 'UNKNOWN'} 评论初筛完成，"
        f"{len(comments)} 条评论，{high_value_count} 条高价值"
    )
    output = {
        "bvid": str(video.get("bvid", "")).strip(),
        "title": str(video.get("title", "")).strip(),
        "comment_count": len(comments),
        "result_count": len(results),
        "high_value_count": high_value_count,
        "results": results,
    }
    with closing(get_conn()) as conn:
        replace_agent_trace_events(
            conn,
            conversation_id=conversation_id,
            run_id=trace.run_id,
            agent_name=trace.agent_name,
            entity_type=trace.entity_type,
            entity_id=trace.entity_id,
            events=trace.all_steps(),
        )
        finish_agent_conversation(
            conn,
            conversation_id,
            status=status,
            summary=summary,
            messages_json=json.dumps(conversation_messages, ensure_ascii=False),
            message_count=len(conversation_messages),
            output_json=json.dumps(output, ensure_ascii=False, default=str),
            public_timeline_json=json.dumps(trace.public_steps(), ensure_ascii=False, default=str),
            raw_timeline_json=json.dumps(trace.all_steps(), ensure_ascii=False, default=str),
            input_summary_json=json.dumps(
                {"bvid": output["bvid"], "comment_count": len(comments)},
                ensure_ascii=False,
                default=str,
            ),
            token_usage_json=json.dumps(trace.token_usage(), ensure_ascii=False, default=str),
            langfuse_trace_id=langfuse_trace_id,
            langfuse_public_url=langfuse_public_url,
            error_message=error_message,
        )


def truncate_text(value: str, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


def build_miner_prompt(video: dict, context: dict, comments: list[str]) -> str:
    tags = video.get("tags", [])
    if not isinstance(tags, list):
        tags = []
    return "\n".join(
        [
            f"BVID: {video.get('bvid', '')}",
            f"标题: {str(video.get('title', '')).strip()[:120]}",
            f"简介: {str(video.get('description', '')).strip()[:300]}",
            f"标签: {', '.join(str(tag).strip() for tag in tags if str(tag).strip()) or '无'}",
            f"视频内容摘要: {str(context.get('summary', '')).strip()[:1000] or '无'}",
            f"视频内容正文: {str(context.get('content_text', '')).strip()[:1200] or '无'}",
            f"字幕摘录: {str(context.get('transcript_excerpt', '')).strip()[:1200] or '无'}",
            "",
            "评论列表：",
            *[f"{index}. {comment}" for index, comment in enumerate(comments)],
        ]
    )


def materialize_insight_record(
    *,
    video: dict,
    context: dict,
    comment_text: str,
    parsed: CommentInsightResult,
    global_index: int,
) -> dict:
    bvid = str(video.get("bvid", "")).strip()
    collected_date = video.get("collected_date")
    comment_hash = hashlib.sha256(
        f"{bvid}|{collected_date}|{global_index}|{comment_text}".encode("utf-8")
    ).hexdigest()
    tags = video.get("tags", [])
    if not isinstance(tags, list):
        tags = []
    is_high_value = (
        float(parsed.confidence) >= settings.miner_comment_confidence_threshold
        and (bool(parsed.is_meme_candidate) or bool(parsed.is_insider_knowledge))
    )
    return {
        "insight_id": comment_hash,
        "bvid": bvid,
        "collected_date": collected_date,
        "partition": str(video.get("partition", "")).strip(),
        "title": str(video.get("title", "")).strip(),
        "description": str(video.get("description", "")).strip(),
        "video_url": str(video.get("url", "")).strip(),
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "comment_text": comment_text,
        "confidence": float(parsed.confidence),
        "is_meme_candidate": bool(parsed.is_meme_candidate),
        "is_insider_knowledge": bool(parsed.is_insider_knowledge),
        "reason": str(parsed.reason).strip(),
        "status": "pending_bundle" if is_high_value else "discarded",
        "video_context": {
            "status": context.get("status", ""),
            "summary": context.get("summary", ""),
            "content_text": context.get("content_text", ""),
            "transcript_excerpt": context.get("transcript_excerpt", ""),
        },
    }
