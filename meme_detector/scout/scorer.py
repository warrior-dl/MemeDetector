"""
Scout 主流程：采集高赞评论 → LLM 识别梗候选 → 存入候选库。
"""

from __future__ import annotations

from datetime import date

from rich.console import Console
from rich.table import Table

from meme_detector.archivist.duckdb_store import get_conn, upsert_scout_candidates
from meme_detector.scout.collector import collect_all_partitions
from meme_detector.scout.llm_analyzer import analyze_all_comments

console = Console()


async def run_scout(target_date: date | None = None) -> list[dict]:
    """
    完整 Scout 流程：
    1. 采集 B站各分区 Top 视频的高赞评论（每视频 top 20）
    2. LLM 批量分析，识别正在传播的梗短语
    3. 写入 DuckDB 候选库，等待 Researcher 深度分析

    返回候选梗列表。
    """
    today = target_date or date.today()
    console.print(f"\n[bold blue]═══ Scout 开始运行 {today} ═══[/bold blue]")

    # 1. 采集
    all_partition_data = await collect_all_partitions()

    all_comments: list[str] = []
    total_videos = 0
    for video_list in all_partition_data.values():
        for vt in video_list:
            all_comments.extend(vt.comments)
            total_videos += 1

    console.print(
        f"\n共采集 {total_videos} 个视频，{len(all_comments)} 条高赞评论"
    )

    if not all_comments:
        console.print("[yellow]无评论数据，跳过分析[/yellow]")
        return []

    # 2. LLM 识别梗
    console.print("\n[bold]LLM 分析中，识别梗候选...[/bold]")
    candidates = await analyze_all_comments(all_comments)

    if not candidates:
        console.print("[yellow]本日未发现梗候选[/yellow]")
        return []

    # 3. 写入候选库
    conn = get_conn()
    upsert_scout_candidates(conn, candidates)
    conn.close()

    # 4. 打印报告
    _print_candidates_table(candidates)
    console.print(
        f"\n[bold green]Scout 完成，共发现 {len(candidates)} 个候选梗[/bold green]"
    )
    return candidates


def _print_candidates_table(candidates: list[dict]) -> None:
    table = Table(title="候选梗", show_lines=True)
    table.add_column("短语", style="bold")
    table.add_column("置信度", justify="right")
    table.add_column("说明")

    for c in candidates[:20]:
        confidence = c.get("confidence", 0)
        table.add_row(
            c.get("phrase", ""),
            f"{confidence:.0%}",
            c.get("explanation", "")[:40],
        )

    console.print(table)
