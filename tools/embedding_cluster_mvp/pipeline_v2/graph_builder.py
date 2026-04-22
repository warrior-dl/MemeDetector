"""Layer 2: 基于 LLM 抽取结果 + 候选词 embedding 构建 Meme 知识图。

图 schema（networkx ``Graph``）：

节点类型（``node_type``）：

- ``candidate``：抽取后聚合的候选片段（文本规范化后的 unique term）
- ``comment``：评论节点
- ``video``：视频节点
- ``author``：作者节点
- ``external_ref``：**预留**——后续接入萌娘百科 / 梗百科时用

边类型（``rel``）：

- ``contains``：comment → candidate
- ``posted_on``：comment → video
- ``posted_by``：comment → author
- ``variant``：candidate ↔ candidate（embedding 余弦相似度高）
- ``co_occurs``：candidate ↔ candidate（共现于同一评论）

Leiden 会在 candidate **子图**（variant ∪ co_occurs）上跑，
其余节点仅作为统计用。
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

import networkx as nx
import numpy as np

from .embedder import cosine_similarity_matrix
from .extractor import ExtractionResult


def _normalize(term: str) -> str:
    """对候选片段做轻量归一：去首尾空白 / 英文大小写统一。
    中文不改字形（"蚌埠住" / "绷不住" 是两个 term，靠 embedding 在图里连起来）。
    """
    return term.strip().lower() if term.isascii() else term.strip()


@dataclass(frozen=True)
class CandidateStats:
    term: str  # 规范化后的文本
    raw_texts: tuple[str, ...]  # 见过的原始形态（大小写变体等）
    freq: int  # 总提及次数
    avg_confidence: float  # LLM 平均把握
    comments: frozenset[str]  # {comment_id} 出现过的评论
    videos: frozenset[str]  # {bvid}
    authors: frozenset[str]  # {mid}


def aggregate_candidates(results: Iterable[ExtractionResult]) -> dict[str, CandidateStats]:
    """仅保留 ``type == meme_candidate`` 的 span，按规范化后的 text 聚合。

    返回 ``{normalized_term: CandidateStats}``。**这一步就扔掉了 platform_term
    / generic_phrase / proper_noun**——对应 v1 方案 TOP 5 FP 的核心过滤点。
    """
    raw_texts: dict[str, set[str]] = defaultdict(set)
    freq = Counter[str]()
    confs: dict[str, list[float]] = defaultdict(list)
    comments: dict[str, set[str]] = defaultdict(set)
    videos: dict[str, set[str]] = defaultdict(set)
    authors: dict[str, set[str]] = defaultdict(set)

    for r in results:
        for span in r.spans:
            if span.type != "meme_candidate":
                continue
            term = _normalize(span.text)
            if not term:
                continue
            raw_texts[term].add(span.text)
            freq[term] += 1
            confs[term].append(span.confidence)
            comments[term].add(r.comment_id)
            videos[term].add(r.bvid)
            if r.mid:
                authors[term].add(r.mid)

    return {
        term: CandidateStats(
            term=term,
            raw_texts=tuple(sorted(raw_texts[term])),
            freq=freq[term],
            avg_confidence=float(np.mean(confs[term])) if confs[term] else 0.0,
            comments=frozenset(comments[term]),
            videos=frozenset(videos[term]),
            authors=frozenset(authors[term]),
        )
        for term in freq
    }


def build_graph(
    results: Iterable[ExtractionResult],
    candidates: dict[str, CandidateStats],
    vectors: dict[str, np.ndarray],
    *,
    variant_sim_threshold: float = 0.82,
    min_candidate_freq: int = 1,
) -> nx.Graph:
    """构图。

    - ``vectors``：{term → embedding}，仅对 candidates 中的 term 需要提供
    - ``variant_sim_threshold``：余弦相似度高于此阈值在候选间加 ``variant`` 边
    - ``min_candidate_freq``：候选词最小出现频次（默认 1——语料小时不能过滤）
    """
    results_list = list(results)

    # 按频次过滤
    kept = {term: stats for term, stats in candidates.items() if stats.freq >= min_candidate_freq and term in vectors}

    g = nx.Graph()

    # 节点：candidate / comment / video / author
    for term, stats in kept.items():
        g.add_node(
            f"cand::{term}",
            node_type="candidate",
            term=term,
            raw_texts=list(stats.raw_texts),
            freq=stats.freq,
            avg_confidence=stats.avg_confidence,
            n_comments=len(stats.comments),
            n_videos=len(stats.videos),
            n_authors=len(stats.authors),
        )

    known_comments: set[str] = set()
    known_videos: set[str] = set()
    known_authors: set[str] = set()
    for r in results_list:
        kept_spans = [s for s in r.spans if s.type == "meme_candidate" and _normalize(s.text) in kept]
        if not kept_spans:
            continue
        cid = f"comment::{r.bvid}::{r.comment_id}"
        if cid not in known_comments:
            g.add_node(cid, node_type="comment", bvid=r.bvid, text=r.text, ctime_iso=r.ctime_iso)
            known_comments.add(cid)
        vid = f"video::{r.bvid}"
        if vid not in known_videos:
            g.add_node(vid, node_type="video", bvid=r.bvid)
            known_videos.add(vid)
        g.add_edge(cid, vid, rel="posted_on")
        if r.mid:
            aid = f"author::{r.mid}"
            if aid not in known_authors:
                g.add_node(aid, node_type="author", mid=r.mid)
                known_authors.add(aid)
            g.add_edge(cid, aid, rel="posted_by")

        # contains 边 + 共现
        terms_in_comment = sorted({_normalize(s.text) for s in kept_spans})
        for term in terms_in_comment:
            g.add_edge(cid, f"cand::{term}", rel="contains")
        for i in range(len(terms_in_comment)):
            for j in range(i + 1, len(terms_in_comment)):
                u = f"cand::{terms_in_comment[i]}"
                v = f"cand::{terms_in_comment[j]}"
                if g.has_edge(u, v):
                    data = g[u][v]
                    if data.get("rel") == "co_occurs":
                        data["weight"] = float(data.get("weight", 0.0)) + 1.0
                        continue
                g.add_edge(u, v, rel="co_occurs", weight=1.0)

    # variant 边
    terms = sorted(kept)
    if len(terms) >= 2:
        matrix = np.stack([vectors[t] for t in terms], axis=0)
        sim = cosine_similarity_matrix(matrix)
        for i in range(len(terms)):
            for j in range(i + 1, len(terms)):
                s = float(sim[i, j])
                if s < variant_sim_threshold:
                    continue
                u = f"cand::{terms[i]}"
                v = f"cand::{terms[j]}"
                if g.has_edge(u, v):
                    data = g[u][v]
                    # variant 边权重以余弦相似度为准；与 co_occurs 并存时合成
                    if data.get("rel") == "co_occurs":
                        data["weight"] = float(data.get("weight", 0.0)) + s
                        data["rel"] = "variant+co_occurs"
                        data["variant_sim"] = s
                    else:
                        data["weight"] = max(float(data.get("weight", 0.0)), s)
                        data["variant_sim"] = s
                else:
                    g.add_edge(u, v, rel="variant", weight=s, variant_sim=s)

    return g


def candidate_subgraph(graph: nx.Graph) -> nx.Graph:
    """抽出只含 candidate 节点 + variant/co_occurs 边的子图供 Leiden 使用。"""
    cand_nodes = [n for n, d in graph.nodes(data=True) if d.get("node_type") == "candidate"]
    sub = graph.subgraph(cand_nodes).copy()
    # 仅保留 candidate-candidate 的加权边
    edges_to_drop = [
        (u, v) for u, v, d in sub.edges(data=True) if d.get("rel") not in {"variant", "co_occurs", "variant+co_occurs"}
    ]
    sub.remove_edges_from(edges_to_drop)
    return sub
