"""Pure-logic 单元测试。不触及 LLM / embedding 外部调用。

测点：

- ``extractor._parse_spans``     JSON 解析 + 类型/置信度校验
- ``graph_builder.aggregate_candidates`` 聚合与类型过滤
- ``graph_builder.build_graph``  节点/边结构 + variant 阈值
- ``community.compute_communities`` 指标计算
- ``evaluation.evaluate``        P/R/F1 + FP 构成
"""

from __future__ import annotations

import numpy as np
import pytest
from pipeline_v2.community import Community, compute_communities, run_leiden
from pipeline_v2.evaluation import evaluate
from pipeline_v2.extractor import ExtractionResult, Span, _parse_spans
from pipeline_v2.graph_builder import (
    aggregate_candidates,
    build_graph,
    candidate_subgraph,
    variant_only_subgraph,
)

# ─────────────────────────── extractor ────────────────────────────


def test_parse_spans_valid_json_object() -> None:
    raw = (
        '{"spans": ['
        '{"text": "家人们谁懂啊", "type": "meme_candidate", "confidence": 0.9},'
        '{"text": "大会员", "type": "platform_term", "confidence": 0.95}'
        "]}"
    )
    spans = _parse_spans(raw)
    assert len(spans) == 2
    assert spans[0].text == "家人们谁懂啊"
    assert spans[0].type == "meme_candidate"
    assert spans[0].confidence == 0.9
    assert spans[1].type == "platform_term"


def test_parse_spans_valid_json_array() -> None:
    raw = '[{"text": "绷不住了", "type": "meme_candidate", "confidence": 0.8}]'
    spans = _parse_spans(raw)
    assert len(spans) == 1
    assert spans[0].text == "绷不住了"


def test_parse_spans_filters_invalid_type() -> None:
    raw = '{"spans":[{"text":"t1","type":"bogus","confidence":0.5}]}'
    assert _parse_spans(raw) == []


def test_parse_spans_clamps_confidence() -> None:
    raw = '{"spans":[{"text":"t","type":"meme_candidate","confidence":1.9}]}'
    spans = _parse_spans(raw)
    assert spans[0].confidence == 1.0


def test_parse_spans_handles_bad_payload() -> None:
    assert _parse_spans("not json at all") == []


# ─────────────────────────── graph_builder ────────────────────────────


def _make_result(
    comment_id: str,
    bvid: str,
    mid: str,
    text: str,
    spans: list[tuple[str, str, float]],
) -> ExtractionResult:
    return ExtractionResult(
        comment_id=comment_id,
        bvid=bvid,
        mid=mid,
        text=text,
        ctime_iso=None,
        spans=[Span(text=t, type=ty, confidence=c) for t, ty, c in spans],  # type: ignore[arg-type]
    )


def test_aggregate_candidates_filters_non_meme_types() -> None:
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [("家人们谁懂啊", "meme_candidate", 0.9), ("大会员", "platform_term", 0.95)],
        ),
        _make_result(
            "c2",
            "BV2",
            "m2",
            "...",
            [("家人们谁懂啊", "meme_candidate", 0.8), ("这个视频", "generic_phrase", 0.9)],
        ),
    ]
    cands = aggregate_candidates(results)
    assert set(cands.keys()) == {"家人们谁懂啊"}
    stats = cands["家人们谁懂啊"]
    assert stats.freq == 2
    assert stats.avg_confidence == pytest.approx(0.85, abs=1e-3)
    assert stats.videos == frozenset({"BV1", "BV2"})
    assert stats.authors == frozenset({"m1", "m2"})
    assert stats.comments == frozenset({"c1", "c2"})


def test_build_graph_creates_variant_edge_above_threshold() -> None:
    results = [
        _make_result("c1", "BV1", "m1", "...", [("绷不住", "meme_candidate", 0.9)]),
        _make_result("c2", "BV2", "m2", "...", [("蚌埠住", "meme_candidate", 0.9)]),
    ]
    candidates = aggregate_candidates(results)
    # 手工构造两个接近的向量：余弦相似度 ≈ 0.99
    v1 = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    v2 = np.array([0.99, 0.1, 0.0], dtype=np.float32)
    vectors = {"绷不住": v1, "蚌埠住": v2}

    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.5)

    # candidate 节点存在
    assert g.nodes["cand::绷不住"]["node_type"] == "candidate"
    assert g.nodes["cand::蚌埠住"]["node_type"] == "candidate"
    # variant 边存在且携带相似度
    assert g.has_edge("cand::绷不住", "cand::蚌埠住")
    edge = g["cand::绷不住"]["cand::蚌埠住"]
    assert edge["rel"] == "variant"
    assert edge["variant_sim"] > 0.9


def test_build_graph_skips_variant_edge_below_threshold() -> None:
    results = [
        _make_result("c1", "BV1", "m1", "...", [("x", "meme_candidate", 0.9)]),
        _make_result("c2", "BV2", "m2", "...", [("y", "meme_candidate", 0.9)]),
    ]
    candidates = aggregate_candidates(results)
    # 几乎正交：余弦相似度 ≈ 0
    vectors = {
        "x": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "y": np.array([0.0, 1.0, 0.0], dtype=np.float32),
    }

    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.5)
    assert not g.has_edge("cand::x", "cand::y")


def test_build_graph_co_occurrence_edge_when_same_comment() -> None:
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [("a", "meme_candidate", 0.9), ("b", "meme_candidate", 0.9)],
        ),
    ]
    candidates = aggregate_candidates(results)
    # 相互正交，variant 边不会建；但因同一评论应有 co_occurs 边
    vectors = {
        "a": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "b": np.array([0.0, 1.0, 0.0], dtype=np.float32),
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.99)
    assert g.has_edge("cand::a", "cand::b")
    assert g["cand::a"]["cand::b"]["rel"] == "co_occurs"
    assert g["cand::a"]["cand::b"]["weight"] == pytest.approx(1.0)


def test_candidate_subgraph_drops_non_candidate_edges() -> None:
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [("a", "meme_candidate", 0.9), ("b", "meme_candidate", 0.9)],
        ),
    ]
    candidates = aggregate_candidates(results)
    vectors = {
        "a": np.array([1.0, 0.0], dtype=np.float32),
        "b": np.array([0.0, 1.0], dtype=np.float32),
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.99)
    sub = candidate_subgraph(g)
    assert set(sub.nodes()) == {"cand::a", "cand::b"}
    # 只剩 candidate-candidate 边
    for _, _, d in sub.edges(data=True):
        assert d.get("rel") in {"co_occurs", "variant", "variant+co_occurs"}


def test_variant_only_subgraph_drops_pure_cooccur_edges() -> None:
    """co_occurs 边会把"同评论共现但语义无关"的词误合并成虚假社区——
    Leiden 子图必须只含 variant 边。"""
    # a,b 语义无关但同评论共现（仅 co_occurs 边）
    # c,d 语义高度相似（variant 边，模拟变体）
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [("a", "meme_candidate", 0.9), ("b", "meme_candidate", 0.9)],
        ),
        _make_result("c2", "BV2", "m2", "...", [("c", "meme_candidate", 0.9)]),
        _make_result("c3", "BV3", "m3", "...", [("d", "meme_candidate", 0.9)]),
    ]
    candidates = aggregate_candidates(results)
    vectors = {
        "a": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "b": np.array([0.0, 1.0, 0.0], dtype=np.float32),  # 与 a 正交
        "c": np.array([0.0, 0.0, 1.0], dtype=np.float32),
        "d": np.array([0.0, 0.05, 0.99], dtype=np.float32),  # 与 c 高相似
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.8)

    sub = variant_only_subgraph(g)
    # 只应保留 c↔d 的 variant 边
    assert sub.has_edge("cand::c", "cand::d")
    assert sub["cand::c"]["cand::d"]["rel"] == "variant"
    # a↔b 的纯 co_occurs 边不该出现
    assert not sub.has_edge("cand::a", "cand::b")
    # 所有剩余边都是 variant 或 variant+co_occurs
    for _, _, d in sub.edges(data=True):
        assert d.get("rel") in {"variant", "variant+co_occurs"}


def test_variant_only_subgraph_keeps_mixed_edges() -> None:
    """若一条边同时有 variant 相似 + 共现，标记为 variant+co_occurs，
    应在 Leiden 子图里**保留**（共现只当辅助信号，不单独驱动社区）。"""
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [("x", "meme_candidate", 0.9), ("y", "meme_candidate", 0.9)],
        ),
    ]
    candidates = aggregate_candidates(results)
    vectors = {
        "x": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "y": np.array([0.99, 0.1, 0.0], dtype=np.float32),  # 高度相似
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.5)
    # 原图里的这条边同时是 variant + co_occurs
    assert g["cand::x"]["cand::y"]["rel"] == "variant+co_occurs"

    sub = variant_only_subgraph(g)
    assert sub.has_edge("cand::x", "cand::y")
    # weight 被回写为 variant_sim（使 Leiden 权重语义一致）
    assert sub["cand::x"]["cand::y"]["weight"] > 0.9


# ─────────────────────────── community ────────────────────────────


def _leiden_or_skip():
    try:
        import igraph  # noqa: F401
        import leidenalg  # noqa: F401
    except ImportError:
        pytest.skip("python-igraph / leidenalg 未安装 (pip install '.[mvp]')")


def test_leiden_separates_disconnected_components() -> None:
    _leiden_or_skip()

    # 两个明显独立的"密集小块"：{a,b,c} 和 {x,y,z}
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [("a", "meme_candidate", 0.9), ("b", "meme_candidate", 0.9), ("c", "meme_candidate", 0.9)],
        ),
        _make_result(
            "c2",
            "BV2",
            "m2",
            "...",
            [("x", "meme_candidate", 0.9), ("y", "meme_candidate", 0.9), ("z", "meme_candidate", 0.9)],
        ),
    ]
    candidates = aggregate_candidates(results)
    vectors = {
        "a": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "b": np.array([0.99, 0.1, 0.0], dtype=np.float32),
        "c": np.array([0.98, 0.2, 0.0], dtype=np.float32),
        "x": np.array([0.0, 0.0, 1.0], dtype=np.float32),
        "y": np.array([0.0, 0.1, 0.99], dtype=np.float32),
        "z": np.array([0.0, 0.2, 0.98], dtype=np.float32),
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.8)
    sub = candidate_subgraph(g)
    membership = run_leiden(sub, seed=42)

    groups: dict[int, set[str]] = {}
    for node, cid in membership.items():
        groups.setdefault(cid, set()).add(node)

    # 应至少把两个块分开
    assert any({"cand::a", "cand::b", "cand::c"}.issubset(g) for g in groups.values())
    assert any({"cand::x", "cand::y", "cand::z"}.issubset(g) for g in groups.values())


def test_compute_communities_aggregates_metrics() -> None:
    _leiden_or_skip()

    results = [
        _make_result("c1", "BV1", "m1", "...", [("绷不住", "meme_candidate", 0.9), ("蚌埠住", "meme_candidate", 0.85)]),
        _make_result("c2", "BV2", "m2", "...", [("绷不住", "meme_candidate", 0.8)]),
    ]
    candidates = aggregate_candidates(results)
    vectors = {
        "绷不住": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "蚌埠住": np.array([0.99, 0.1, 0.0], dtype=np.float32),
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.5)
    sub = candidate_subgraph(g)
    membership = run_leiden(variant_only_subgraph(g), seed=42)
    comm_list = compute_communities(sub, membership, candidates, {})

    assert len(comm_list) >= 1
    big = max(comm_list, key=lambda c: c.size)
    assert big.size == 2
    assert big.total_freq == 3  # 绷不住 × 2 + 蚌埠住 × 1
    assert big.n_videos == 2
    assert big.n_authors == 2
    assert big.n_comments == 2
    assert 0.0 <= big.avg_variant_sim <= 1.0
    # 两 term 同评论共现 1 次，应反映在 cooccur_event_count 上
    assert big.cooccur_edge_count >= 1
    assert big.cooccur_event_count >= 1


def test_leiden_ignores_cooccur_edges() -> None:
    """v2 关键回归：两个语义无关的 term 即便同评论共现，Leiden 也不应把它们合并。

    场景：{华强买瓜} + {按在地上摩擦} 在同一条评论里共现（co_occurs 边 clique），
    但 embedding 完全正交。Leiden 输入用 variant_only_subgraph 后，
    不同 term 应落入不同社区。
    """
    _leiden_or_skip()

    # 同一条评论里 4 个语义无关的 term 共现，形成完整 4-clique 的 co_occurs 边
    results = [
        _make_result(
            "c1",
            "BV1",
            "m1",
            "...",
            [
                ("华强买瓜", "meme_candidate", 0.9),
                ("按在地上摩擦", "meme_candidate", 0.9),
                ("撸网贷", "meme_candidate", 0.9),
                ("吃瓜", "meme_candidate", 0.9),
            ],
        ),
    ]
    candidates = aggregate_candidates(results)
    # 四个完全正交的向量 → 没有 variant 边
    vectors = {
        "华强买瓜": np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
        "按在地上摩擦": np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
        "撸网贷": np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32),
        "吃瓜": np.array([0.0, 0.0, 0.0, 1.0], dtype=np.float32),
    }
    g = build_graph(results, candidates, vectors, variant_sim_threshold=0.5)

    # 旧行为：直接在含 co_occurs 的子图跑 Leiden → 4 个 term 被合并成一大社区
    bad_sub = candidate_subgraph(g)
    bad_membership = run_leiden(bad_sub, seed=42)
    bad_groups = len(set(bad_membership.values()))
    assert bad_groups == 1, "控制样本：co_occurs 驱动下 Leiden 合并为单社区（v2_run_002 的 bug）"

    # 新行为：只喂 variant 边 → 没有 variant 边，每个 term 应落单
    good_sub = variant_only_subgraph(g)
    good_membership = run_leiden(good_sub, seed=42)
    good_groups = len(set(good_membership.values()))
    assert good_groups == 4, "修复后：纯共现不再驱动合并，4 个无关 term 应分成 4 个社区"


# ─────────────────────────── evaluation ────────────────────────────


def _make_comm(community_id: int, terms: list[str], verdict: str) -> Community:
    return Community(
        community_id=community_id,
        terms=terms,
        size=len(terms),
        total_freq=len(terms),
        n_comments=1,
        n_videos=1,
        n_authors=1,
        avg_confidence=0.9,
        internal_density=0.9,
        avg_variant_sim=0.9,
        burst_score=0.5,
        cross_video_ratio=0.5,
        verdict=verdict,
        verdict_reason="test",
        verdict_confidence=0.9,
    )


def test_evaluate_precision_recall() -> None:
    # 三条评论：
    #   c1/BV1 含 "绷不住"    gold=1  meme社区命中  → TP
    #   c2/BV2 含 "大会员"    gold=1  meme社区没命中 → FN
    #   c3/BV3 含 "绷不住"    gold=0  meme社区命中  → FP
    results = [
        _make_result("c1", "BV1", "m1", "...", [("绷不住", "meme_candidate", 0.9)]),
        _make_result("c2", "BV2", "m2", "...", [("大会员", "meme_candidate", 0.9)]),
        _make_result("c3", "BV3", "m3", "...", [("绷不住", "meme_candidate", 0.9)]),
    ]
    communities = [
        _make_comm(0, ["绷不住"], "meme"),
        _make_comm(1, ["大会员"], "not_meme"),
    ]
    gold = {
        ("c1", "BV1"): 1,
        ("c2", "BV2"): 1,
        ("c3", "BV3"): 0,
    }
    m = evaluate(results, communities, gold)
    assert m.gold_total == 3
    assert m.gold_positive == 2
    assert m.true_positive == 1
    assert m.false_positive == 1
    assert m.false_negative == 1
    assert m.precision == pytest.approx(0.5, abs=1e-3)
    assert m.recall == pytest.approx(0.5, abs=1e-3)
    assert m.f1 == pytest.approx(0.5, abs=1e-3)


def test_evaluate_no_predictions() -> None:
    results = [
        _make_result("c1", "BV1", "m1", "...", [("绷不住", "meme_candidate", 0.9)]),
    ]
    # 所有社区都 not_meme，相当于系统不给出 meme 判定
    communities = [_make_comm(0, ["绷不住"], "not_meme")]
    gold = {("c1", "BV1"): 1}
    m = evaluate(results, communities, gold)
    assert m.true_positive == 0
    assert m.false_negative == 1
    assert m.precision == 0.0
    assert m.recall == 0.0
