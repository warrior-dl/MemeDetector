"""
Research Step 2/3: 深度分析、来源验证辅助与 Agent 对话落库。
"""

from __future__ import annotations

import json
from datetime import date

from openai import AsyncOpenAI
from pydantic_ai import Agent, capture_run_messages, messages
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.deepseek import DeepSeekProvider
from pydantic_ai.providers.moonshotai import MoonshotAIProvider
from pydantic_ai.providers.openai import OpenAIProvider
from tenacity import retry, stop_after_attempt, wait_exponential

from meme_detector.archivist.duckdb_store import (
    create_agent_conversation,
    finish_agent_conversation,
    get_conn,
)
from meme_detector.config import settings
from meme_detector.llm_factory import build_openai_chat_model, build_provider
from meme_detector.logging_utils import get_logger
from meme_detector.miner.video_context import get_bilibili_video_context
from meme_detector.researcher.models import MemeRecord
from meme_detector.researcher.tools import (
    volcengine_web_search,
    volcengine_web_search_summary,
)
from meme_detector.run_tracker import get_current_run_id

logger = get_logger(__name__)


def build_research_provider(
    *,
    client: AsyncOpenAI,
    model_name: str,
    base_url: str,
) -> OpenAIProvider | DeepSeekProvider | MoonshotAIProvider:
    return build_provider(
        client=client,
        model_name=model_name,
        base_url=base_url,
        provider_hint="auto",
    )


def get_research_model() -> OpenAIChatModel:
    return build_openai_chat_model(
        "research",
        timeout=settings.research_llm_timeout_seconds,
        max_retries=settings.research_llm_max_retries,
    )


_DEEP_ANALYSIS_SYSTEM = """\
你是一位专业的互联网亚文化研究员，正在为一个梗百科数据库撰写词条。
你会收到 Research 阶段整理出的评论样本，以及系统预取的“评论对应视频”背景和外部搜索上下文。
你有权调用火山引擎联网搜索工具来补充查阅资料。

请按以下步骤工作：
1. 先阅读 prompt 中已经提供的评论样本、关联视频背景、外部搜索上下文，它们优先级最高
2. 如果系统预取的总结版搜索内容已经足够解释来源、语义和传播背景，就不要重复搜索
3. 只有在系统预取信息不足、来源冲突、缺少原始出处细节时，再调用 volcengine_web_search_summary 或 volcengine_web_search
4. 综合所有信息，填写完整的词条

输出要求：
- definition: 简洁解释含义，说明在网络上如何使用，不超过100字
- origin: 明确说明来源视频/事件，如有 BV 号请写入 source_urls
- category: 从[抽象、谐音、游戏、影视、音乐、社会现象、二次元、其他]中选
- heat_index: 根据搜索结果中的播放量/讨论量估算，0-100
- lifecycle_stage: emerging（最近才出现）/ peak（正在高峰）/ declining（已过热度）

关于系统预取的视频背景：
- status=ready 时，优先使用 summary / chapters / transcript_excerpt 作为视频背景
- status=skipped 且 skip_reason=duration_exceeded 时，表示视频超过 15 分钟，忽略即可
- status=unavailable 或 status=error 时，不要编造视频内容
"""

deep_agent: Agent[None, MemeRecord] = Agent(
    model=get_research_model(),
    output_type=MemeRecord,
    system_prompt=_DEEP_ANALYSIS_SYSTEM,
    tools=[volcengine_web_search_summary, volcengine_web_search],  # type: ignore[arg-type]
)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=3, max=15))
async def deep_analyze(
    word: str,
    sample_comments: str,
    video_refs: list[dict],
    score: float,
    today: date,
) -> MemeRecord | None:
    """对单个候选词进行深度分析。"""
    heat = min(100, int(score / 10 * 30 + 40)) if score < 999 else 75
    run_id = get_current_run_id()
    conversation_id: str | None = None
    if run_id:
        conn = get_conn()
        conversation_id = create_agent_conversation(
            conn,
            run_id=run_id,
            agent_name="researcher",
            word=word,
        )
        conn.close()

    logger.info(
        "research deep analysis context preparation started",
        extra={
            "event": "research_deep_analysis_context_preparation_started",
            "word": word,
            "conversation_id": conversation_id,
            "score": score,
            "video_count": len(video_refs),
        },
    )
    video_contexts = await prepare_linked_video_contexts(video_refs)
    external_search_context = await prepare_external_search_context(word)
    logger.info(
        "research deep analysis context prepared",
        extra={
            "event": "research_deep_analysis_context_prepared",
            "word": word,
            "conversation_id": conversation_id,
            "video_count": len(video_contexts),
            "summary_sufficient": external_search_context.get("summary_sufficient", False),
            "result_count": len(external_search_context.get("web_results", [])),
        },
    )
    prompt = (
        f'请为网络梗词汇「{word}」撰写完整词条。\n\n'
        f'检测信息：\n'
        f'- Research 候选得分：{score:.1f}/100\n'
        f'- 检测日期：{today}\n'
        f'- B站评论示例：\n{sample_comments or "（无样本）"}\n\n'
        f'- Scout 关联视频背景：\n{format_video_contexts(video_contexts)}\n\n'
        f'- 外部搜索上下文：\n{format_external_search_context(external_search_context)}\n\n'
        f'请优先结合这些系统预取上下文，再按需调用搜索工具后输出完整词条。'
    )

    try:
        with capture_run_messages() as captured_messages:
            result = await deep_agent.run(prompt)
        record = result.output
        record.id = word
        record.first_detected_at = today
        record.updated_at = today
        record.heat_index = max(record.heat_index, heat)
        logger.info(
            "research deep analysis succeeded",
            extra={
                "event": "research_deep_analysis_succeeded",
                "word": word,
                "conversation_id": conversation_id,
                "source_count": len(record.source_urls),
                "result_count": len(record.category),
            },
        )
        if conversation_id:
            persist_agent_conversation(
                conversation_id=conversation_id,
                status="success",
                summary=record.title or record.definition[:80],
                messages_json=result.all_messages_json().decode("utf-8"),
                message_count=len(result.all_messages()),
                output_json=json.dumps(record.model_dump(), ensure_ascii=False, default=str),
            )
        return record
    except Exception as exc:
        logger.exception(
            "research deep analysis failed",
            extra={
                "event": "research_deep_analysis_failed",
                "word": word,
                "conversation_id": conversation_id,
            },
        )
        if conversation_id:
            failure_messages = captured_messages if "captured_messages" in locals() else []
            persist_agent_conversation(
                conversation_id=conversation_id,
                status="failed",
                summary=f"{word} 分析失败",
                messages_json=serialize_messages(failure_messages),
                message_count=len(failure_messages),
                error_message=str(exc),
            )
        return None


def persist_agent_conversation(
    *,
    conversation_id: str,
    status: str,
    summary: str,
    messages_json: str,
    message_count: int,
    output_json: str = "{}",
    error_message: str = "",
) -> None:
    conn = get_conn()
    finish_agent_conversation(
        conn,
        conversation_id,
        status=status,
        summary=summary,
        messages_json=messages_json,
        message_count=message_count,
        output_json=output_json,
        error_message=error_message,
    )
    conn.close()


def serialize_messages(model_messages: list) -> str:
    if not model_messages:
        return "[]"
    return messages.ModelMessagesTypeAdapter.dump_json(model_messages).decode("utf-8")


async def prepare_linked_video_contexts(
    video_refs: list[dict],
    limit: int = 2,
) -> list[dict]:
    prepared: list[dict] = []
    for video_ref in select_linked_videos(video_refs, limit=limit):
        bvid = str(video_ref.get("bvid", "")).strip()
        if not bvid:
            continue
        logger.info(
            "research linked video context started",
            extra={
                "event": "research_linked_video_context_started",
                "bvid": bvid,
                "word": str(video_ref.get("word", "")).strip(),
            },
        )
        try:
            context = await get_bilibili_video_context(bvid)
        except Exception as exc:
            context = {
                "bvid": bvid,
                "status": "error",
                "error": str(exc),
            }
        prepared.append(
            {
                "video_ref": video_ref,
                "context": context,
            }
        )
        logger.info(
            "research linked video context completed",
            extra={
                "event": "research_linked_video_context_completed",
                "bvid": bvid,
                "status": context.get("status", ""),
            },
        )
    return prepared


async def prepare_external_search_context(word: str) -> dict:
    query = f"{word} 梗 来源"
    logger.info(
        "research external search started",
        extra={
            "event": "research_external_search_started",
            "word": word,
        },
    )
    summary_result = await volcengine_web_search_summary(query, num_results=5)
    summary_ok = isinstance(summary_result, dict) and "error" not in summary_result
    summary_sufficient = summary_ok and is_summary_search_sufficient(summary_result)

    web_results: list[dict] = []
    if not summary_sufficient:
        web_results = await volcengine_web_search(query, num_results=5)
    logger.info(
        "research external search completed",
        extra={
            "event": "research_external_search_completed",
            "word": word,
            "summary_sufficient": summary_sufficient,
            "result_count": len(web_results),
        },
    )

    return {
        "query": query,
        "summary_result": summary_result,
        "summary_sufficient": summary_sufficient,
        "web_results": web_results,
    }


def is_summary_search_sufficient(summary_result: dict) -> bool:
    summary_text = str(summary_result.get("summary", "")).strip()
    results = summary_result.get("results", [])
    if not isinstance(results, list):
        results = []

    if len(summary_text) >= 80:
        return True

    rich_results = 0
    for item in results:
        if not isinstance(item, dict):
            continue
        snippet = str(item.get("snippet", "")).strip()
        content = str(item.get("content", "")).strip()
        if len(content) >= 80 or len(snippet) >= 50:
            rich_results += 1

    if len(summary_text) >= 50 and rich_results >= 1:
        return True

    return rich_results >= 2 or (summary_text and rich_results >= 1)


def select_linked_videos(video_refs: list[dict], limit: int = 2) -> list[dict]:
    normalized = [item for item in video_refs if isinstance(item, dict) and item.get("bvid")]
    normalized.sort(
        key=lambda item: int(item.get("matched_comment_count", 0)),
        reverse=True,
    )
    return normalized[:limit]


def format_video_contexts(video_contexts: list[dict]) -> str:
    if not video_contexts:
        return "（无与评论直接关联的视频上下文）"

    sections: list[str] = []
    for index, item in enumerate(video_contexts, 1):
        video_ref = item.get("video_ref", {})
        context = item.get("context", {})
        matched_comments = video_ref.get("matched_comments", [])
        if not isinstance(matched_comments, list):
            matched_comments = []

        lines = [
            (
                f"{index}. "
                f"{video_ref.get('title') or context.get('title') or video_ref.get('bvid', '')}"
            ),
            f"   BVID: {video_ref.get('bvid', '')}",
            f"   URL: {video_ref.get('url') or context.get('video_url', '')}",
            f"   分区: {video_ref.get('partition', '')}",
            f"   匹配评论数: {video_ref.get('matched_comment_count', 0)}",
            f"   视频背景状态: {context.get('status', 'unknown')}",
        ]

        if matched_comments:
            lines.append("   匹配评论样本:")
            lines.extend(f"   - {comment}" for comment in matched_comments[:3])

        status = context.get("status")
        if status == "ready":
            summary = str(context.get("summary", "")).strip()
            transcript = str(context.get("transcript_excerpt", "")).strip()
            chapters = context.get("chapters", [])
            if summary:
                lines.append(f"   Bibi 摘要: {summary[:600]}")
            if transcript:
                lines.append(f"   字幕摘录: {transcript[:600]}")
            if isinstance(chapters, list) and chapters:
                chapter_summary = " | ".join(
                    f"{chapter.get('timestamp', '')} {chapter.get('title', '')}".strip()
                    for chapter in chapters[:4]
                    if isinstance(chapter, dict)
                )
                if chapter_summary:
                    lines.append(f"   章节: {chapter_summary}")
        elif status == "skipped":
            lines.append(f"   跳过原因: {context.get('skip_reason', 'unknown')}")
        elif status == "unavailable":
            lines.append(f"   不可用原因: {context.get('skip_reason', 'missing_api_token')}")
        elif status == "error":
            lines.append(f"   错误: {context.get('error', 'unknown')}")

        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def format_external_search_context(search_context: dict) -> str:
    if not isinstance(search_context, dict):
        return "（无外部搜索上下文）"

    lines = [f"查询词: {search_context.get('query', '')}"]
    summary_result = search_context.get("summary_result", {})
    summary_sufficient = bool(search_context.get("summary_sufficient"))

    if isinstance(summary_result, dict) and summary_result.get("error"):
        lines.append(f"总结版搜索错误: {summary_result['error']}")
    else:
        lines.append(f"总结版搜索是否足够: {'是' if summary_sufficient else '否'}")
        summary_text = ""
        if isinstance(summary_result, dict):
            summary_text = str(summary_result.get("summary", "")).strip()
        lines.append(f"总结版摘要: {summary_text or '（无）'}")

        summary_items = []
        if isinstance(summary_result, dict):
            raw_results = summary_result.get("results", [])
            if isinstance(raw_results, list):
                summary_items = raw_results[:3]

        if summary_items:
            lines.append("总结版来源:")
            for index, item in enumerate(summary_items, 1):
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                link = str(item.get("link", "")).strip()
                snippet = str(item.get("snippet", "")).strip()
                content = str(item.get("content", "")).strip()
                lines.append(f"{index}. {title or '未命名结果'}")
                if link:
                    lines.append(f"   链接: {link}")
                if snippet:
                    lines.append(f"   摘要: {snippet[:300]}")
                if content:
                    lines.append(f"   正文摘录: {content[:500]}")

    web_results = search_context.get("web_results", [])
    if isinstance(web_results, list) and web_results:
        lines.append("普通网页搜索补充:")
        for index, item in enumerate(web_results[:3], 1):
            if not isinstance(item, dict):
                continue
            if item.get("error"):
                lines.append(f"{index}. 错误: {item['error']}")
                continue
            lines.append(f"{index}. {str(item.get('title', '')).strip() or '未命名结果'}")
            link = str(item.get("link", "")).strip()
            if link:
                lines.append(f"   链接: {link}")
            snippet = str(item.get("snippet", "")).strip()
            if snippet:
                lines.append(f"   摘要: {snippet[:300]}")

    return "\n".join(lines)
