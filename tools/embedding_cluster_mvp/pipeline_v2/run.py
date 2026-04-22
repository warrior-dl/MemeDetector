"""Meme KG MVP v2 orchestrator.

一键跑完：load → extract → embed → build_graph → Leiden → judge → evaluate → report。

使用：

.. code-block:: bash

    python tools/embedding_cluster_mvp/pipeline_v2/run.py \\
        --db data/duckdb/freq.db \\
        --out-dir tools/embedding_cluster_mvp/data/v2_run_001

产出：

- ``extracted.jsonl``           — LLM 抽取原始结果（可复跑时 cache）
- ``embeddings.jsonl``           — 候选词 embedding cache
- ``graph.gexf``                 — 图结构（可导入 Gephi 可视化）
- ``communities.json``           — Leiden 社区 + 每簇裁决
- ``eval_report.md``             — 最终评估报告
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import networkx as nx

from meme_detector.logging_utils import get_logger

from .community import (
    compute_communities,
    describe_community,
    run_leiden,
    summarize_graph,
)
from .embedder import embed_texts
from .evaluation import evaluate, load_gold
from .extractor import (
    cached_keys,
    extract_spans,
    load_cache,
    save_cache,
)
from .graph_builder import (
    aggregate_candidates,
    build_graph,
    candidate_subgraph,
    variant_only_subgraph,
)
from .judge import judge_communities
from .load_corpus import load_comments

logger = get_logger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


async def main_async(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    extract_cache = out_dir / "extracted.jsonl"
    embed_cache = out_dir / "embeddings.jsonl"
    graph_path = out_dir / "graph.gexf"
    communities_path = out_dir / "communities.json"
    report_path = out_dir / "eval_report.md"
    run_meta_path = out_dir / "run_meta.json"

    run_started_at = _utc_now_iso()
    logger.info(
        "mvp_v2 run start",
        extra={"event": "mvp_v2_start", "out_dir": str(out_dir), "db": args.db},
    )

    # ── Step 1: Load corpus ─────────────────────────────────────────────────
    comments = load_comments(args.db, limit=args.limit)
    print(f"[1/7] 拉评论：{len(comments)} 条")
    if not comments:
        print("没有评论，退出。")
        return 1

    # ── Step 2: LLM extraction（带缓存）─────────────────────────────────────
    existing = load_cache(extract_cache)
    done_keys = cached_keys(existing)
    pending = [c for c in comments if c.key not in done_keys]
    print(f"[2/7] LLM 抽取：已缓存 {len(existing)} 条，新抽 {len(pending)} 条")
    if pending:
        new_results = await extract_spans(
            pending,
            concurrency=args.extract_concurrency,
            target=args.extract_target,  # type: ignore[arg-type]
        )
        all_results = existing + list(new_results)
        save_cache(all_results, extract_cache)
    else:
        all_results = existing

    errors = sum(1 for r in all_results if r.error)
    total_spans = sum(len(r.spans) for r in all_results)
    meme_candidate_spans = sum(1 for r in all_results for s in r.spans if s.type == "meme_candidate")
    print(f"     抽取 spans 总数={total_spans} 其中 meme_candidate={meme_candidate_spans} 失败评论={errors}")

    # ── Step 3: Aggregate candidates + embed ────────────────────────────────
    candidates = aggregate_candidates(all_results)
    print(f"[3/7] 聚合候选：{len(candidates)} 个 unique meme_candidate term")
    if not candidates:
        print("没有 meme_candidate，退出。")
        return 1

    terms = sorted(candidates.keys())
    vectors_list = await embed_texts(
        terms,
        cache_path=embed_cache,
        concurrency=args.embed_concurrency,
    )
    vectors = dict(zip(terms, vectors_list, strict=True))
    dim = len(vectors_list[0]) if vectors_list else 0
    print(f"     embedding：{len(vectors)} 个 term × {dim} 维")

    # ── Step 4: Build graph ─────────────────────────────────────────────────
    graph = build_graph(
        all_results,
        candidates,
        vectors,
        variant_sim_threshold=args.variant_threshold,
        min_candidate_freq=args.min_freq,
    )
    graph_summary = summarize_graph(graph)
    print(f"[4/7] 图构建：{graph_summary}")
    # GEXF 不支持嵌套 list；把 raw_texts 扁平化成字符串后再写
    graph_for_export = graph.copy()
    for _, data in graph_for_export.nodes(data=True):
        if isinstance(data.get("raw_texts"), list):
            data["raw_texts"] = " | ".join(data["raw_texts"])
    try:
        nx.write_gexf(graph_for_export, graph_path)
    except Exception as exc:  # noqa: BLE001  - gexf 导出只是辅助，不中断主流程
        logger.warning("gexf export failed", extra={"event": "gexf_export_fail", "error": repr(exc)})

    # 统计子图（含 variant + co_occurs 边，用于密度 / 共现辅助统计）
    stats_sub = candidate_subgraph(graph)
    # Leiden 子图（只含 variant 边，避免 co_occurs 驱动的虚假合并）
    leiden_sub = variant_only_subgraph(graph)
    print(
        f"     Leiden 输入子图：nodes={leiden_sub.number_of_nodes()} "
        f"edges={leiden_sub.number_of_edges()} (仅 variant 边)"
    )

    # ── Step 5: Leiden community detection ──────────────────────────────────
    membership = run_leiden(leiden_sub, resolution=args.leiden_resolution, seed=args.seed)
    comment_ctimes = {r.comment_id: r.ctime_iso for r in all_results}
    communities = compute_communities(stats_sub, membership, candidates, comment_ctimes)
    print(f"[5/7] Leiden 社区：{len(communities)} 个")
    for c in communities[: args.print_top]:
        print("     " + describe_community(c))

    # ── Step 6: LLM judge ───────────────────────────────────────────────────
    print(f"[6/7] LLM 裁决：对 {len(communities)} 个社区逐个判定 ...")
    communities = await judge_communities(
        communities,
        concurrency=args.judge_concurrency,
        min_size=args.judge_min_size,
        target=args.judge_target,  # type: ignore[arg-type]
    )

    # 写 communities.json
    communities_payload = {
        "generated_at": _utc_now_iso(),
        "total": len(communities),
        "items": [asdict(c) for c in communities],
    }
    communities_path.write_text(
        json.dumps(communities_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # ── Step 7: Evaluate + report ───────────────────────────────────────────
    gold = load_gold(Path(args.gold))
    metrics = evaluate(all_results, communities, gold) if gold else None

    meme_communities = [c for c in communities if c.verdict == "meme"]
    not_meme_communities = [c for c in communities if c.verdict == "not_meme"]
    uncertain_communities = [c for c in communities if c.verdict == "uncertain"]

    report_lines: list[str] = []
    report_lines.append("# Eval Report (MVP v2)\n")
    report_lines.append("## Run\n")
    report_lines.append(f"- Generated at: {_utc_now_iso()}")
    report_lines.append(f"- Started at:   {run_started_at}")
    report_lines.append(f"- Comments:     {len(comments)}")
    report_lines.append(f"- Extraction errors: {errors}")
    report_lines.append(f"- Spans total:  {total_spans}  (meme_candidate={meme_candidate_spans})")
    report_lines.append(f"- Candidates:   {len(candidates)}")
    for k, v in graph_summary.items():
        report_lines.append(f"- {k}: {v}")
    report_lines.append(f"- Communities:  {len(communities)}")
    report_lines.append(
        f"- Verdicts:     meme={len(meme_communities)} "
        f"not_meme={len(not_meme_communities)} "
        f"uncertain={len(uncertain_communities)}\n"
    )

    def _block(title: str, items: list) -> None:
        report_lines.append(f"## {title}\n")
        if not items:
            report_lines.append("_(空)_\n")
            return
        for c in items[: args.report_top]:
            reason = c.verdict_reason or ""
            conf = c.verdict_confidence if c.verdict_confidence is not None else 0.0
            terms_preview = ", ".join(c.terms[:8])
            if len(c.terms) > 8:
                terms_preview += f", … ({len(c.terms)})"
            report_lines.append(
                f"- **#{c.community_id}** `{c.verdict}` conf={conf:.2f} "
                f"size={c.size} freq={c.total_freq} "
                f"videos={c.n_videos} authors={c.n_authors} "
                f"variant_sim={c.avg_variant_sim:.2f} cv_ratio={c.cross_video_ratio:.2f}"
            )
            report_lines.append(f"  - terms: {terms_preview}")
            if reason:
                report_lines.append(f"  - reason: {reason}")
        report_lines.append("")

    _block("Meme communities (Layer 4 verdict = meme)", meme_communities)
    _block("Not-meme communities", not_meme_communities)
    _block("Uncertain communities", uncertain_communities)

    report_lines.append("## Metrics\n")
    if metrics is None:
        report_lines.append("- Gold dataset missing (`data/gold.csv`)，跳过 P/R 计算。")
        report_lines.append("- 运行 `python tools/embedding_cluster_mvp/annotate.py` 先做标注。\n")
    else:
        report_lines.append(f"- Gold total:         {metrics.gold_total}")
        report_lines.append(f"- Gold is_meme=1:     {metrics.gold_positive}")
        report_lines.append(f"- Gold is_meme=0:     {metrics.gold_negative}")
        report_lines.append(f"- Predicted positive: {metrics.predicted_positive}")
        report_lines.append(f"- True positive:      {metrics.true_positive}")
        report_lines.append(f"- False positive:     {metrics.false_positive}")
        report_lines.append(f"- False negative:     {metrics.false_negative}")
        report_lines.append(f"- **Precision**: {metrics.precision:.3f}")
        report_lines.append(f"- **Recall**:    {metrics.recall:.3f}")
        report_lines.append(f"- **F1**:        {metrics.f1:.3f}")
        if metrics.fp_composition:
            report_lines.append("\nFP span-type 构成：")
            for t, n in sorted(metrics.fp_composition.items(), key=lambda x: -x[1]):
                report_lines.append(f"- {t}: {n}")

    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"[7/7] 报告：{report_path}")

    run_meta_path.write_text(
        json.dumps(
            {
                "started_at": run_started_at,
                "finished_at": _utc_now_iso(),
                "args": vars(args),
                "comments": len(comments),
                "candidates": len(candidates),
                "communities": len(communities),
                "graph_summary": graph_summary,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Meme KG MVP v2 orchestrator")
    p.add_argument("--db", default="data/duckdb/freq.db", help="DuckDB 路径")
    p.add_argument(
        "--out-dir",
        default="tools/embedding_cluster_mvp/data/v2_run",
        help="产出目录（缓存 + 报告 + 图）",
    )
    p.add_argument("--gold", default="tools/embedding_cluster_mvp/data/gold.csv")
    p.add_argument("--limit", type=int, default=None, help="只拉前 N 条评论（调试用）")

    p.add_argument("--extract-target", default="miner", choices=["default", "miner", "research"])
    p.add_argument("--extract-concurrency", type=int, default=8)
    p.add_argument("--embed-concurrency", type=int, default=4)
    p.add_argument("--judge-target", default="research", choices=["default", "miner", "research"])
    p.add_argument("--judge-concurrency", type=int, default=6)
    p.add_argument("--judge-min-size", type=int, default=1)

    p.add_argument(
        "--variant-threshold",
        type=float,
        default=0.75,
        help="variant 边余弦阈值（0.70 偏松易把 doge↔单身狗 这种同语义类误连；0.82 偏严"
        "错过 绷不住↔没绷住 这种真变体；0.75 经验折中）",
    )
    p.add_argument("--min-freq", type=int, default=1, help="candidate 最小提及频次")
    p.add_argument("--leiden-resolution", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=42)

    p.add_argument("--print-top", type=int, default=10)
    p.add_argument("--report-top", type=int, default=30)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
