"""Layer 5 (optional): 若存在 ``data/gold.csv`` 则计算 comment-level P/R。

没有 gold 就跳过——MVP 首次跑时用户尚未做标注，允许这一路不出数。
"""

from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from .community import Community
from .extractor import ExtractionResult


@dataclass
class EvalMetrics:
    gold_total: int
    gold_positive: int
    gold_negative: int
    predicted_positive: int
    true_positive: int
    false_positive: int
    false_negative: int
    precision: float
    recall: float
    f1: float
    fp_composition: dict[str, int]  # LLM 抽取时的 span type 分布


def load_gold(path: Path) -> dict[tuple[str, str], int]:
    """读 ``gold.csv``（annotate.py 产出），返回 ``{(comment_id, bvid): is_meme 0/1}``。"""
    if not path.exists():
        return {}
    out: dict[tuple[str, str], int] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            cid = str(row.get("comment_id") or "").strip()
            bvid = str(row.get("bvid") or "").strip()
            if not cid or not bvid:
                continue
            label_raw = str(row.get("is_meme") or "").strip()
            if label_raw not in {"0", "1"}:
                continue
            out[(cid, bvid)] = int(label_raw)
    return out


def _meme_terms(communities: list[Community]) -> set[str]:
    """收集所有被判为 ``meme`` 的社区里的 term 集合。"""
    out: set[str] = set()
    for c in communities:
        if c.verdict == "meme":
            out.update(c.terms)
    return out


def _normalize(text: str) -> str:
    return text.strip().lower() if text.isascii() else text.strip()


def evaluate(
    results: list[ExtractionResult],
    communities: list[Community],
    gold: dict[tuple[str, str], int],
) -> EvalMetrics:
    """把评论级预测 = 「该评论含任意"meme 社区"中的 term」与 gold 对齐。"""
    meme_terms = _meme_terms(communities)

    predicted: dict[tuple[str, str], set[str]] = {}  # comment → matched terms
    span_type_by_comment: dict[tuple[str, str], list[str]] = {}
    for r in results:
        key = (r.comment_id, r.bvid)
        hit_terms = {
            _normalize(s.text) for s in r.spans if s.type == "meme_candidate" and _normalize(s.text) in meme_terms
        }
        if hit_terms:
            predicted[key] = hit_terms
        span_type_by_comment[key] = [s.type for s in r.spans]

    gold_total = len(gold)
    gold_positive = sum(1 for v in gold.values() if v == 1)
    gold_negative = gold_total - gold_positive

    tp = 0
    fp = 0
    fn = 0
    fp_composition = Counter[str]()
    for key, label in gold.items():
        is_predicted = key in predicted
        if label == 1 and is_predicted:
            tp += 1
        elif label == 1 and not is_predicted:
            fn += 1
        elif label == 0 and is_predicted:
            fp += 1
            # FP 的"LLM 抽出什么类型的 span" 分布，帮定位误判根因
            for t in span_type_by_comment.get(key, []):
                fp_composition[t] += 1

    predicted_positive = sum(1 for k in predicted if k in gold)
    precision = tp / predicted_positive if predicted_positive else 0.0
    recall = tp / gold_positive if gold_positive else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    return EvalMetrics(
        gold_total=gold_total,
        gold_positive=gold_positive,
        gold_negative=gold_negative,
        predicted_positive=predicted_positive,
        true_positive=tp,
        false_positive=fp,
        false_negative=fn,
        precision=precision,
        recall=recall,
        f1=f1,
        fp_composition=dict(fp_composition),
    )
