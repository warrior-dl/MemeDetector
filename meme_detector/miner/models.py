"""
Miner 阶段核心模型。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class MinerInsightsRunResult(BaseModel):
    """Miner Stage 1：评论初筛运行结果。"""

    target_date: str
    video_count: int = 0
    insight_count: int = 0
    high_value_count: int = 0
    failed_video_count: int = 0

    def __getitem__(self, key: str):
        return getattr(self, key)

    def get(self, key: str, default=None):
        return getattr(self, key, default)


class MinerBundlesRunResult(BaseModel):
    """Miner Stage 2：证据包生成运行结果。"""

    target_date: str
    queued_insight_count: int = 0
    bundled_count: int = 0
    failed_insight_count: int = 0

    def __getitem__(self, key: str):
        return getattr(self, key)

    def get(self, key: str, default=None):
        return getattr(self, key, default)


class MinerRunResult(BaseModel):
    """兼容性的组合结果，用于串行执行两个 Miner 阶段。"""

    target_date: str
    video_count: int = 0
    insight_count: int = 0
    high_value_count: int = 0
    bundle_count: int = 0
    failed_video_count: int = 0


class CommentInsightResult(BaseModel):
    """单条评论的初步线索判定。"""

    index: int = Field(ge=0, description="评论在当前批次中的索引")
    is_meme_candidate: bool = Field(description="是否像潜在梗表达")
    is_insider_knowledge: bool = Field(description="是否像圈内知识/圈层黑话")
    confidence: float = Field(ge=0.0, le=1.0, description="初步判定置信度")
    reason: str = Field(description="简短理由")
