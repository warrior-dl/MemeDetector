"""V3 候选发现层（candidate_discovery）。

目录规划（随 M1-b / M1-c / M2 里程碑推进）::

    candidate_discovery/
    ├── burst_detector.py    # M1-b：弹幕共鸣爆点
    ├── repeat_detector.py   # M2-a：评论复读（未实现）
    ├── graph_builder.py     # M1-c：KG 构建 + variant_only_subgraph（未实现）
    ├── judge.py             # M1-c：pairwise judge，替代 miner.scorer（未实现）
    └── run.py               # M1-c：把 scout → candidate_discovery → researcher
                             #        串成一个生产 Job（未实现）

本 package 当前只暴露 burst_detector 的公开符号，其余会在对应 PR 中陆续加入。
"""

from meme_detector.candidate_discovery.burst_detector import (
    BurstEvent,
    detect_burst_events,
)

__all__ = ["BurstEvent", "detect_burst_events"]
