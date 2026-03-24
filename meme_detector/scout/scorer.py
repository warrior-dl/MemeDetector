"""
Scout 主流程：采集 → 分词 → 存储词频 → 生成候选词。
"""

from __future__ import annotations

from datetime import date

from rich.console import Console
from rich.table import Table

from meme_detector.archivist.duckdb_store import (
    compute_candidates,
    get_conn,
    update_candidate_comments,
    upsert_word_freq,
)
from meme_detector.scout.collector import collect_all_partitions
from meme_detector.scout.segmenter import compute_word_freq, extract_sample_comments

console = Console()


async def run_scout(target_date: date | None = None) -> list[dict]:
    """
    完整 Scout 流程：
    1. 采集 B站各分区评论/弹幕
    2. 分词统计词频，写入 DuckDB
    3. 计算环比 Score，生成候选词列表

    返回候选词列表。
    """
    today = target_date or date.today()
    console.print(f"\n[bold blue]═══ Scout 开始运行 {today} ═══[/bold blue]")

    # 1. 采集
    all_partition_data = await collect_all_partitions()

    conn = get_conn()

    # 2. 分词 + 写入词频
    all_texts_by_partition: dict[str, list[str]] = {}

    for partition_name, video_list in all_partition_data.items():
        texts = []
        for vt in video_list:
            texts.extend(vt.all_texts)
        all_texts_by_partition[partition_name] = texts

        if not texts:
            console.print(f"[yellow]分区 {partition_name} 无文本，跳过[/yellow]")
            continue

        console.print(
            f"\n[cyan]分词中: {partition_name}，共 {len(texts)} 条文本...[/cyan]"
        )
        freq_records = compute_word_freq(texts)
        console.print(f"  → 得到 {len(freq_records)} 个词")

        upsert_word_freq(conn, freq_records, partition=partition_name, target_date=today)

    # 3. 计算候选词
    console.print("\n[bold]计算环比 Score，生成候选词...[/bold]")
    candidates = compute_candidates(conn, current_date=today)

    if not candidates:
        console.print("[yellow]本日无候选词（可能是冷启动期，数据积累中）[/yellow]")
        conn.close()
        return []

    # 4. 为候选词采集样本评论（供 AI 分析上下文）
    all_texts_flat = [t for texts in all_texts_by_partition.values() for t in texts]
    for c in candidates:
        sample = extract_sample_comments(c["word"], all_texts_flat)
        if sample:
            update_candidate_comments(conn, c["word"], sample)

    conn.close()

    # 5. 打印报告
    _print_candidates_table(candidates)

    console.print(f"\n[bold green]Scout 完成，共发现 {len(candidates)} 个候选词[/bold green]")
    return candidates


def _print_candidates_table(candidates: list[dict]) -> None:
    table = Table(title="候选梗词", show_lines=True)
    table.add_column("词", style="bold")
    table.add_column("Score", justify="right")
    table.add_column("类型")
    table.add_column("当日频次", justify="right")

    for c in candidates[:20]:  # 最多显示 20 个
        score_str = "∞ (新词)" if c["is_new_word"] else f"{c['score']:.1f}x"
        word_type = "[magenta]新词[/magenta]" if c["is_new_word"] else "老词"
        table.add_row(c["word"], score_str, word_type, str(c["curr_freq"]))

    console.print(table)
