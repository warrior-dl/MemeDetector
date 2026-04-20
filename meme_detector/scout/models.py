"""
Scout 阶段结果模型。
"""

from __future__ import annotations

from pydantic import BaseModel


class ScoutRunResult(BaseModel):
    """Scout 流程运行结果。"""

    target_date: str
    video_count: int = 0
    comment_count: int = 0
