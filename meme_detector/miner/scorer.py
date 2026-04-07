"""
Miner 主流程：预取视频内容并对评论做初步线索打分。
"""

from __future__ import annotations

import hashlib
import json
from datetime import date

from openai import AsyncOpenAI
from rich.console import Console

from meme_detector.archivist.duckdb_store import (
    create_agent_conversation,
    finish_agent_conversation,
    get_conn,
    get_pending_scout_raw_videos,
    mark_scout_raw_videos_mined,
    upsert_miner_comment_insights,
)
from meme_detector.config import settings
from meme_detector.miner.models import CommentInsightResult
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.run_tracker import get_current_run_id

console = Console()

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


async def run_miner(target_date: date | None = None) -> dict:
    """对待处理的 Scout 原始视频做评论线索打分。"""
    today = target_date or date.today()
    console.print(f"\n[bold blue]═══ Miner 开始运行 {today} ═══[/bold blue]")

    conn = get_conn()
    pending_videos = get_pending_scout_raw_videos(conn)
    conn.close()
    if not pending_videos:
        console.print("[yellow]暂无待挖掘的 Scout 原始视频[/yellow]")
        return {
            "target_date": today.isoformat(),
            "video_count": 0,
            "insight_count": 0,
            "high_value_count": 0,
        }

    conn = get_conn()
    insight_count = 0
    high_value_count = 0
    try:
        for video_index, video in enumerate(pending_videos, 1):
            comments = video.get("comments", [])
            if not isinstance(comments, list):
                comments = []
            comments = [str(comment).strip() for comment in comments if str(comment).strip()]
            if not comments:
                mark_scout_raw_videos_mined(conn, [video])
                console.print(
                    f"[cyan]处理视频 {video_index}/{len(pending_videos)}: "
                    f"{str(video.get('bvid', '')).strip() or 'UNKNOWN'} "
                    "[dim](无有效评论，已跳过并落库状态)[/dim][/cyan]"
                )
                continue

            console.print(
                f"[cyan]处理视频 {video_index}/{len(pending_videos)}: "
                f"{str(video.get('bvid', '')).strip() or 'UNKNOWN'} "
                f"({len(comments)} 条评论)[/cyan]"
            )
            insights = await _score_video_comments(video, comments)
            upsert_miner_comment_insights(conn, insights)
            mark_scout_raw_videos_mined(conn, [video])
            insight_count += len(insights)
            high_value_count += sum(
                1
                for item in insights
                if item.get("confidence", 0.0) >= settings.miner_comment_confidence_threshold
                and (item.get("is_meme_candidate") or item.get("is_insider_knowledge"))
            )
    finally:
        conn.close()

    console.print(
        f"[bold green]Miner 完成，写入 {insight_count} 条评论线索，"
        f"其中 {high_value_count} 条高价值[/bold green]"
    )
    return {
        "target_date": today.isoformat(),
        "video_count": len(pending_videos),
        "insight_count": insight_count,
        "high_value_count": high_value_count,
    }


async def _score_video_comments(video: dict, comments: list[str]) -> list[dict]:
    conversation_id = _create_miner_conversation(video)
    conversation_messages: list[dict] = []
    context = await get_bilibili_video_context(str(video.get("bvid", "")).strip())
    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        timeout=settings.miner_llm_timeout_seconds,
        max_retries=max(settings.miner_llm_max_retries, 0),
    )

    chunks = [
        comments[i : i + settings.miner_comments_batch_size]
        for i in range(0, len(comments), settings.miner_comments_batch_size)
    ]
    all_results: list[dict] = []
    try:
        for chunk_index, chunk in enumerate(chunks):
            offset = chunk_index * settings.miner_comments_batch_size
            fallback_reason = "模型未返回有效结果"
            user_msg = _build_miner_prompt(video, context, chunk)
            conversation_messages.extend(
                [
                    {
                        "role": "system",
                        "chunk_index": chunk_index,
                        "content": _truncate_text(_MINER_SYSTEM, 3000),
                    },
                    {
                        "role": "user",
                        "chunk_index": chunk_index,
                        "content": _truncate_text(user_msg, 12000),
                    },
                ]
            )
            try:
                raw = await _request_chunk_comment_scores(
                    client=client,
                    user_msg=user_msg,
                )
                conversation_messages.append(
                    {
                        "role": "assistant",
                        "chunk_index": chunk_index,
                        "content": _truncate_text(raw, 12000),
                    }
                )
                items = _extract_chunk_items(raw)
            except Exception as exc:
                fallback_reason = _format_chunk_failure_reason(exc)
                console.print(
                    f"[yellow]  评论批次 {chunk_index + 1}/{len(chunks)} 分析失败: "
                    f"{_summarize_exception(exc)}[/yellow]"
                )
                conversation_messages.append(
                    {
                        "role": "assistant",
                        "chunk_index": chunk_index,
                        "error": _summarize_exception(exc),
                    }
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
                    _materialize_insight_record(
                        video=video,
                        context=context,
                        comment_text=comment_text,
                        parsed=parsed,
                        global_index=offset + local_index,
                    )
                )
    except Exception as exc:
        _persist_miner_conversation(
            conversation_id=conversation_id,
            status="failed",
            video=video,
            comments=comments,
            results=all_results,
            conversation_messages=conversation_messages,
            error_message=str(exc),
        )
        raise

    _persist_miner_conversation(
        conversation_id=conversation_id,
        status="success",
        video=video,
        comments=comments,
        results=all_results,
        conversation_messages=conversation_messages,
    )
    return all_results


async def _request_chunk_comment_scores(
    *,
    client: AsyncOpenAI,
    user_msg: str,
) -> str:
    resp = await client.chat.completions.create(
        model=settings.deepseek_model,
        messages=[
            {"role": "system", "content": _MINER_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
    )
    return resp.choices[0].message.content or "{}"


def _extract_chunk_items(raw: str) -> list[dict]:
    data = json.loads(raw)
    items = data if isinstance(data, list) else data.get("results", [])
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _format_chunk_failure_reason(exc: Exception) -> str:
    summary = _summarize_exception(exc)
    return f"模型请求失败: {summary}"[:120]


def _summarize_exception(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        return exc.__class__.__name__
    return message[:80]


def _create_miner_conversation(video: dict) -> str | None:
    run_id = get_current_run_id()
    if not run_id:
        return None
    conn = get_conn()
    try:
        return create_agent_conversation(
            conn,
            run_id=run_id,
            agent_name="miner",
            word=str(video.get("bvid", "")).strip() or str(video.get("title", "")).strip() or "UNKNOWN",
        )
    finally:
        conn.close()


def _persist_miner_conversation(
    *,
    conversation_id: str | None,
    status: str,
    video: dict,
    comments: list[str],
    results: list[dict],
    conversation_messages: list[dict],
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
    conn = get_conn()
    try:
        finish_agent_conversation(
            conn,
            conversation_id,
            status=status,
            summary=summary,
            messages_json=json.dumps(conversation_messages, ensure_ascii=False),
            message_count=len(conversation_messages),
            output_json=json.dumps(output, ensure_ascii=False, default=str),
            error_message=error_message,
        )
    finally:
        conn.close()


def _truncate_text(value: str, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


def _build_miner_prompt(video: dict, context: dict, comments: list[str]) -> str:
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


def _materialize_insight_record(
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
        "video_context": {
            "status": context.get("status", ""),
            "summary": context.get("summary", ""),
            "content_text": context.get("content_text", ""),
            "transcript_excerpt": context.get("transcript_excerpt", ""),
        },
    }
