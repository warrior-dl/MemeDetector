#!/usr/bin/env python3
"""Gold 标注工具：从 DuckDB 的 scout_raw_comments 随机抽样，让用户做 is_meme 二分类标注。

使用：
    python tools/embedding_cluster_mvp/annotate.py \\
        --db data/duckdb/freq.db \\
        --out tools/embedding_cluster_mvp/data/gold.csv \\
        --n 200 --seed 42

支持断点续标：每标一条立即 flush 到 CSV，下次启动自动跳过已标的 (rpid, bvid)。

交互命令：
    1  = 标 is_meme=1（真梗）
    0  = 标 is_meme=0（非梗）
    s  = 跳过当前评论（不写入 CSV，继续下一条）
    b  = 撤销上一条已标（重写 CSV 并把那条塞回待标队列）
    q  = 保存并退出
    ?  = 显示帮助
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

HEADER = [
    "comment_id",
    "bvid",
    "mid",
    "uname",
    "text",
    "is_meme",
    "note",
    "labeled_at",
]

HELP = """
命令：
  1  = is_meme=1（真梗）
  0  = is_meme=0（非梗）
  s  = 跳过（不写入 CSV，继续下一条）
  b  = 撤销上一条（从 CSV 删除并把那条塞回队列）
  q  = 保存并退出
  ?  = 显示本帮助

标注后可紧接着输入备注（可选，回车跳过）。
"""


def load_pool(db_path: str, min_len: int, max_len: int, seed: int) -> list[dict[str, str]]:
    """从 DuckDB 拉评论池，按 seed 做确定性 shuffle。"""
    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT
                CAST(rpid AS VARCHAR) AS comment_id,
                bvid,
                CAST(COALESCE(mid, 0) AS VARCHAR) AS mid,
                COALESCE(uname, '') AS uname,
                message AS text
            FROM scout_raw_comments
            WHERE message IS NOT NULL
              AND length(message) BETWEEN ? AND ?
            """,
            [min_len, max_len],
        ).fetchall()
    finally:
        conn.close()

    pool = [
        {
            "comment_id": r[0],
            "bvid": r[1],
            "mid": r[2],
            "uname": r[3],
            "text": r[4],
        }
        for r in rows
    ]
    rng = random.Random(seed)
    rng.shuffle(pool)
    return pool


def load_existing(out_path: Path) -> tuple[list[dict[str, str]], set[tuple[str, str]]]:
    """读已存在的 gold CSV，返回全部记录 + 已标 (comment_id, bvid) 集合。"""
    if not out_path.exists():
        return [], set()
    records: list[dict[str, str]] = []
    keys: set[tuple[str, str]] = set()
    with out_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
            keys.add((row["comment_id"], row["bvid"]))
    return records, keys


def append_row(out_path: Path, row: dict[str, str]) -> None:
    """追加一条记录到 CSV，写完立即 flush，防止意外退出丢数据。"""
    is_new = not out_path.exists()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        if is_new:
            writer.writeheader()
        writer.writerow(row)
        f.flush()


def rewrite_all(out_path: Path, records: list[dict[str, str]]) -> None:
    """全量重写 CSV（用于撤销）。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(records)


def show_comment(idx: int, total_done: int, target: int, item: dict[str, str]) -> None:
    print("\n" + "=" * 70)
    print(f"进度：{total_done}/{target}   池位置：{idx}")
    print(f"BV  : {item['bvid']}")
    print(f"RPID: {item['comment_id']}")
    print(f"UP  : {item['uname']} (mid={item['mid']})")
    print("-" * 70)
    print(item["text"])
    print("=" * 70)


def prompt_label() -> str | None:
    """返回 '1' / '0' / 's' / 'b' / 'q'；None 表示 EOF。"""
    while True:
        try:
            cmd = input("[1=梗 0=非 s=跳 b=撤 q=退 ?=帮助] > ").strip().lower()
        except EOFError:
            return None
        if cmd in {"1", "0", "s", "b", "q"}:
            return cmd
        if cmd in {"?", "h", "help"}:
            print(HELP)
            continue
        print("  无效指令，输入 ? 看帮助")


def prompt_note() -> str:
    try:
        return input("备注（回车跳过）> ").strip()
    except EOFError:
        return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Gold 标注工具 (is_meme 二分类)")
    parser.add_argument("--db", default="data/duckdb/freq.db", help="DuckDB 路径")
    parser.add_argument(
        "--out",
        default="tools/embedding_cluster_mvp/data/gold.csv",
        help="Gold CSV 输出路径",
    )
    parser.add_argument("--n", type=int, default=200, help="目标标注数量")
    parser.add_argument("--seed", type=int, default=42, help="随机种子（决定抽样顺序）")
    parser.add_argument("--min-len", type=int, default=2, help="评论最短字符数")
    parser.add_argument("--max-len", type=int, default=300, help="评论最长字符数")
    args = parser.parse_args()

    out_path = Path(args.out)
    existing_records, labeled_keys = load_existing(out_path)
    total_done = len(existing_records)
    print(f"已有标注：{total_done} 条 → {out_path}")
    if total_done >= args.n:
        print(f"已达到目标 {args.n}，如需更多请把 --n 调大后再运行。")
        return 0

    print(f"从 {args.db} 读取评论池...")
    pool = load_pool(args.db, args.min_len, args.max_len, args.seed)
    print(f"评论池大小：{len(pool)}（按 seed={args.seed} 打乱）")
    if not pool:
        print("评论池为空，请确认 DB 路径和 scout_raw_comments 表内容。")
        return 1

    remaining = [item for item in pool if (item["comment_id"], item["bvid"]) not in labeled_keys]
    print(f"剩余未标：{len(remaining)}")

    print(HELP)

    idx = 0
    while total_done < args.n:
        if idx >= len(remaining):
            print("\n评论池已遍历完，提前结束。考虑放宽 --min-len / --max-len，或先 scout 更多数据。")
            break
        item = remaining[idx]
        show_comment(idx, total_done, args.n, item)
        cmd = prompt_label()
        if cmd is None or cmd == "q":
            print("\n已保存，退出。")
            break
        if cmd == "s":
            idx += 1
            continue
        if cmd == "b":
            if not existing_records:
                print("没有可撤销的条目。")
                continue
            removed = existing_records.pop()
            rewrite_all(out_path, existing_records)
            labeled_keys.discard((removed["comment_id"], removed["bvid"]))
            total_done -= 1
            remaining.insert(
                idx,
                {
                    "comment_id": removed["comment_id"],
                    "bvid": removed["bvid"],
                    "mid": removed["mid"],
                    "uname": removed["uname"],
                    "text": removed["text"],
                },
            )
            print(f"已撤销：rpid={removed['comment_id']} bvid={removed['bvid']}")
            continue
        # "1" 或 "0"
        note = prompt_note()
        row = {
            "comment_id": item["comment_id"],
            "bvid": item["bvid"],
            "mid": item["mid"],
            "uname": item["uname"],
            "text": item["text"],
            "is_meme": cmd,
            "note": note,
            "labeled_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        append_row(out_path, row)
        existing_records.append(row)
        labeled_keys.add((item["comment_id"], item["bvid"]))
        total_done += 1
        idx += 1

    print(f"\n最终标注：{total_done} 条  →  {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
