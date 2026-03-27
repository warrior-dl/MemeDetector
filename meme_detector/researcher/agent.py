"""
AI 分析模块：三步流程对候选词进行梗识别和溯源。

Step 1: DeepSeek 批量快速筛选（低成本）
Step 2: 深度分析 + 工具调用（仅高置信度候选词）
Step 3: 来源 URL 验证（防幻觉）
"""

from __future__ import annotations

import json
from datetime import date

from openai import AsyncOpenAI
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from rich.console import Console
from rich.progress import track
from tenacity import retry, stop_after_attempt, wait_exponential

from meme_detector.archivist.duckdb_store import (
    get_conn,
    get_pending_candidates,
    update_candidate_status,
)
from meme_detector.archivist.meili_store import upsert_meme
from meme_detector.config import settings
from meme_detector.researcher.models import MemeRecord, QuickScreenResult
from meme_detector.researcher.tools import bilibili_search, verify_urls, web_search

console = Console()

# ── 模型初始化 ──────────────────────────────────────────────

def _get_deepseek_model() -> OpenAIModel:
    return OpenAIModel(
        model_name=settings.deepseek_model,
        openai_client=AsyncOpenAI(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
        ),
    )


# ── Step 1: 快速批量筛选 ────────────────────────────────────

_SCREEN_SYSTEM = """\
你是一位专业的互联网亚文化研究员，专注于识别中文网络梗和亚文化词汇。

判断标准（符合任意一条即为梗）：
1. 谐音/谐意：利用汉字谐音创造的新含义（如"依托答辩"=依托大便）
2. 二次元出典：来源于动漫、游戏、小说的台词或梗
3. 社会事件：因某个热点事件催生的特定用语
4. 抽象文化：B站鬼畜、抽象文化圈的专属词汇
5. 圈内黑话：某个内容圈子（游戏圈、美食圈等）专有词汇

不是梗的情况：
- 普通流行语（"内卷"、"躺平"已是通用词）
- 活动关键词（活动名称、UP主名字）
- 普通口语（"真的"、"确实"、"好吧"）

few-shot 示例：
- "依托答辩" → is_meme=true，谐音梗
- "遥遥领先" → is_meme=true，因某事件语境下二次流行
- "内卷" → is_meme=false，已成主流词汇
- "哈哈哈" → is_meme=false，普通口语
- "这波" → is_meme=false，普通网络用语
"""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def _batch_screen(
    candidates: list[dict],
) -> list[QuickScreenResult]:
    """批量快速筛选，每批最多 AI_BATCH_SIZE 个词。"""
    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
    )

    word_list = []
    for c in candidates:
        sample = c.get("sample_comments", "").strip()
        explanation = c.get("explanation", "").strip()
        context = explanation if explanation else (sample[:150] if sample else "无")
        word_list.append(
            f'- 词: "{c["word"]}" | 上下文: {context}'
        )

    user_msg = (
        "请对以下词汇逐一判断是否为网络梗，返回 JSON 数组，"
        "每项格式：{word, is_meme, confidence, candidate_category, reason}\n\n"
        + "\n".join(word_list)
    )

    resp = await client.chat.completions.create(
        model=settings.deepseek_model,
        messages=[
            {"role": "system", "content": _SCREEN_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    raw = resp.choices[0].message.content or "{}"
    data = json.loads(raw)

    # 兼容返回 {"results": [...]} 或直接 [...]
    items = data if isinstance(data, list) else data.get("results", [])
    results = []
    for item in items:
        try:
            results.append(QuickScreenResult(**item))
        except Exception:
            pass
    return results


# ── Step 2: 深度分析 Agent ──────────────────────────────────

_DEEP_ANALYSIS_SYSTEM = """\
你是一位专业的互联网亚文化研究员，正在为一个梗百科数据库撰写词条。
你有权调用 B站搜索 和 Web搜索 工具来查阅资料。

请按以下步骤工作：
1. 先调用 bilibili_search 搜索该词，了解相关视频
2. 再调用 web_search 搜索 "[词] 梗 来源" 获取背景
3. 综合所有信息，填写完整的词条

输出要求：
- definition: 简洁解释含义，说明在网络上如何使用，不超过100字
- origin: 明确说明来源视频/事件，如有 BV 号请写入 source_urls
- category: 从[抽象、谐音、游戏、影视、音乐、社会现象、二次元、其他]中选
- heat_index: 根据搜索结果中的播放量/讨论量估算，0-100
- lifecycle_stage: emerging（最近才出现）/ peak（正在高峰）/ declining（已过热度）
"""

deep_agent: Agent[None, MemeRecord] = Agent(
    model=_get_deepseek_model(),
    result_type=MemeRecord,
    system_prompt=_DEEP_ANALYSIS_SYSTEM,
    tools=[bilibili_search, web_search],  # type: ignore[arg-type]
)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=3, max=15))
async def _deep_analyze(
    word: str,
    sample_comments: str,
    score: float,
    today: date,
) -> MemeRecord | None:
    """对单个候选词进行深度分析。"""
    heat = min(100, int(score / 10 * 30 + 40)) if score < 999 else 75

    prompt = (
        f'请为网络梗词汇「{word}」撰写完整词条。\n\n'
        f'检测信息：\n'
        f'- 词频增长倍数：{"新词首次出现" if score >= 999 else f"{score:.1f}x"}\n'
        f'- 检测日期：{today}\n'
        f'- B站评论示例：\n{sample_comments or "（无样本）"}\n\n'
        f'请调用搜索工具后输出完整词条。'
    )

    try:
        result = await deep_agent.run(prompt)
        record = result.data
        # 补充自动字段
        record.id = word
        record.first_detected_at = today
        record.updated_at = today
        record.heat_index = max(record.heat_index, heat)
        return record
    except Exception as e:
        console.print(f"[red]  深度分析失败 [{word}]: {e}[/red]")
        return None


# ── 主流程 ──────────────────────────────────────────────────

async def run_research() -> dict:
    """完整的 AI 分析流程。"""
    console.print("\n[bold blue]═══ Researcher 开始运行 ═══[/bold blue]")

    conn = get_conn()
    candidates = get_pending_candidates(conn, limit=settings.ai_batch_size)
    result = {
        "pending_count": len(candidates),
        "screened_count": 0,
        "deep_analysis_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "accepted_records": [],
        "rejected_words": [],
        "failed_words": [],
    }

    if not candidates:
        console.print("[yellow]暂无待分析候选词[/yellow]")
        conn.close()
        return result

    console.print(f"共 {len(candidates)} 个候选词待分析")

    # ── Step 1: 批量快速筛选 ─────────────────────────────────
    console.print("\n[bold]Step 1: 快速批量筛选...[/bold]")
    screen_results = await _batch_screen(candidates)
    result["screened_count"] = len(screen_results)

    screen_map = {r.word: r for r in screen_results}
    to_deep = [
        c for c in candidates
        if screen_map.get(c["word"]) and
           screen_map[c["word"]].is_meme and
           screen_map[c["word"]].confidence >= settings.ai_confidence_threshold
    ]

    rejected = [
        c["word"] for c in candidates
        if c["word"] not in {x["word"] for x in to_deep}
    ]
    for word in rejected:
        update_candidate_status(conn, word, "rejected")
    result["rejected_words"] = rejected
    result["rejected_count"] = len(rejected)
    result["deep_analysis_count"] = len(to_deep)

    console.print(
        f"  筛选结果：[green]{len(to_deep)} 个通过[/green]，"
        f"[red]{len(rejected)} 个拒绝[/red]"
    )

    if not to_deep:
        conn.close()
        return result

    # ── Step 2 & 3: 深度分析 + URL 验证 ──────────────────────
    console.print("\n[bold]Step 2: 深度分析 + 溯源...[/bold]")
    today = date.today()
    success_count = 0

    for c in track(to_deep, description="分析中..."):
        word = c["word"]
        screen = screen_map.get(word)
        console.print(f"\n  → [{word}] confidence={screen.confidence:.2f}")

        record = await _deep_analyze(
            word=word,
            sample_comments=c.get("sample_comments", ""),
            score=c["score"],
            today=today,
        )
        if record is None:
            result["failed_words"].append(word)
            continue

        # Step 3: URL 验证
        if record.source_urls:
            original_source_count = len(record.source_urls)
            valid_urls = await verify_urls(record.source_urls)
            console.print(
                f"     来源验证：{original_source_count} → {len(valid_urls)} 个有效"
            )
            record.source_urls = valid_urls
            # 有效来源少于预期时，适当降低置信度
            if original_source_count > 0 and len(valid_urls) < original_source_count / 2:
                record.confidence_score *= 0.8

        # 写入 Meilisearch
        await upsert_meme(record)
        update_candidate_status(conn, word, "accepted")
        success_count += 1
        result["accepted_records"].append(
            {
                "id": record.id,
                "title": record.title,
                "heat_index": record.heat_index,
                "lifecycle_stage": record.lifecycle_stage,
                "confidence_score": record.confidence_score,
            }
        )
        console.print("     [green]✓ 已入库[/green]")

    conn.close()
    result["accepted_count"] = success_count
    console.print(
        f"\n[bold green]Researcher 完成：{success_count} 个梗成功入库[/bold green]"
    )
    return result
