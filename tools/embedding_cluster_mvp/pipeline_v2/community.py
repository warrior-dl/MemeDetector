"""Layer 3: Leiden 社区发现 + 社区级指标计算。

对 candidate 子图跑 Leiden，得到 ``community_id`` → 一组 term。
然后为每个社区计算可解释的汇总指标，供 Layer 4 LLM judge / 打分参考。
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import mean, stdev

import networkx as nx
import numpy as np

from .graph_builder import CandidateStats


@dataclass
class Community:
    community_id: int
    terms: list[str]
    size: int
    total_freq: int
    n_comments: int
    n_videos: int
    n_authors: int
    avg_confidence: float
    # 簇内平均边权（variant/co-occurrence 综合强度）
    internal_density: float
    # 簇内节点的 variant_sim 平均（仅 variant 边）
    avg_variant_sim: float
    # 时间集中度：越大 = 越短期爆发（log inverse time span in hours）
    burst_score: float
    # cross_video_ratio = n_videos / total_freq；越低（<<1）= 越集中，
    # 越高（→1）= 跨视频分布越均匀（通用词特征）
    cross_video_ratio: float
    # 裁决相关字段（Layer 4 会填）
    verdict: str | None = None  # "meme" / "not_meme" / "uncertain"
    verdict_reason: str | None = None
    verdict_confidence: float | None = None


def run_leiden(subgraph: nx.Graph, *, resolution: float = 1.0, seed: int = 42) -> dict[str, int]:
    """返回 ``{node_id: community_id}`` 映射。

    - 要求 ``python-igraph`` + ``leidenalg`` 已安装
    - ``resolution``：越大社区越小；1.0 是标准 modularity 分辨率
    """
    if subgraph.number_of_nodes() == 0:
        return {}

    # Lazy import 避免在没装 mvp 依赖时影响其它模块 import
    import igraph as ig
    import leidenalg

    nodes = list(subgraph.nodes())
    idx = {n: i for i, n in enumerate(nodes)}
    edges: list[tuple[int, int]] = []
    weights: list[float] = []
    for u, v, d in subgraph.edges(data=True):
        edges.append((idx[u], idx[v]))
        weights.append(float(d.get("weight", 1.0)))

    g_ig = ig.Graph(n=len(nodes), edges=edges, directed=False)
    g_ig.es["weight"] = weights

    partition = leidenalg.find_partition(
        g_ig,
        leidenalg.RBConfigurationVertexPartition,
        weights="weight",
        resolution_parameter=resolution,
        seed=seed,
    )
    return {nodes[i]: int(partition.membership[i]) for i in range(len(nodes))}


def _burst_score(ctimes_iso: list[str]) -> float:
    """基于时间集中度的爆发分。时间跨度越小（短期集中）→ 分数越高。
    无时间戳 / 单条时给 0."""
    from datetime import datetime

    dts: list[datetime] = []
    for s in ctimes_iso:
        if not s:
            continue
        try:
            dts.append(datetime.fromisoformat(s))
        except ValueError:
            continue
    if len(dts) < 2:
        return 0.0
    span_hours = (max(dts) - min(dts)).total_seconds() / 3600
    if span_hours <= 0:
        return 1.0
    # 1 / (1 + log(hours))；1 小时 → 1.0，24 小时 → ~0.24，30 天 → ~0.13
    return float(1.0 / (1.0 + np.log1p(span_hours)))


def compute_communities(
    subgraph: nx.Graph,
    membership: dict[str, int],
    candidate_stats: dict[str, CandidateStats],
    comment_ctimes: dict[str, str | None],
) -> list[Community]:
    """聚合 community 级别指标。

    Args:
        subgraph: candidate 子图（只含 candidate 节点）
        membership: {node_id: community_id}
        candidate_stats: {term: CandidateStats}（来自 aggregate_candidates）
        comment_ctimes: {comment_id: ctime_iso}；用于 burst 计算
    """
    groups: dict[int, list[str]] = {}
    for node, cid in membership.items():
        groups.setdefault(cid, []).append(node)

    out: list[Community] = []
    for cid, nodes in groups.items():
        terms = [subgraph.nodes[n]["term"] for n in nodes]
        stats_list = [candidate_stats[t] for t in terms if t in candidate_stats]
        if not stats_list:
            continue

        comments_union: set[str] = set()
        videos_union: set[str] = set()
        authors_union: set[str] = set()
        confs: list[float] = []
        total_freq = 0
        for s in stats_list:
            comments_union |= set(s.comments)
            videos_union |= set(s.videos)
            authors_union |= set(s.authors)
            confs.append(s.avg_confidence)
            total_freq += s.freq

        # 密度 & variant sim
        sub2 = subgraph.subgraph(nodes)
        weights = [float(d.get("weight", 0.0)) for _, _, d in sub2.edges(data=True)]
        variant_sims = [
            float(d.get("variant_sim")) for _, _, d in sub2.edges(data=True) if d.get("variant_sim") is not None
        ]
        internal_density = mean(weights) if weights else 0.0
        avg_variant_sim = mean(variant_sims) if variant_sims else 0.0

        # burst：该社区所有评论的 ctime
        ctimes = [comment_ctimes.get(cmid) or "" for cmid in comments_union]
        burst = _burst_score(ctimes)

        cross_video_ratio = (len(videos_union) / total_freq) if total_freq else 0.0

        out.append(
            Community(
                community_id=int(cid),
                terms=sorted(set(terms)),
                size=len(nodes),
                total_freq=total_freq,
                n_comments=len(comments_union),
                n_videos=len(videos_union),
                n_authors=len(authors_union),
                avg_confidence=mean(confs) if confs else 0.0,
                internal_density=float(internal_density),
                avg_variant_sim=float(avg_variant_sim),
                burst_score=float(burst),
                cross_video_ratio=float(cross_video_ratio),
            )
        )

    # 按大小降序（size desc, freq desc），稳定输出
    out.sort(key=lambda c: (c.size, c.total_freq), reverse=True)
    return out


def summarize_graph(graph: nx.Graph) -> dict[str, int]:
    """跑完 build_graph 后输出一份基础统计。"""
    by_type = Counter(d.get("node_type") for _, d in graph.nodes(data=True))
    by_rel = Counter(d.get("rel") for _, _, d in graph.edges(data=True))
    return {
        "nodes_total": graph.number_of_nodes(),
        "edges_total": graph.number_of_edges(),
        **{f"nodes_{k}": int(v) for k, v in by_type.items() if k},
        **{f"edges_{k}": int(v) for k, v in by_rel.items() if k},
    }


def describe_community(c: Community) -> str:
    """用于在日志 / 报告里一行展示社区摘要。"""
    terms_preview = ", ".join(c.terms[:6])
    if len(c.terms) > 6:
        terms_preview += ", …"
    return (
        f"#{c.community_id} size={c.size} freq={c.total_freq} "
        f"videos={c.n_videos} authors={c.n_authors} "
        f"density={c.internal_density:.2f} variant_sim={c.avg_variant_sim:.2f} "
        f"burst={c.burst_score:.2f} cv_ratio={c.cross_video_ratio:.2f} "
        f"[{terms_preview}]"
    )


# stdev 暴露给 test，避免 "unused" 导入
_ = stdev
