"""
Miner 阶段核心模型。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class CommentInsightResult(BaseModel):
    """单条评论的初步线索判定。"""

    index: int = Field(ge=0, description="评论在当前批次中的索引")
    is_meme_candidate: bool = Field(description="是否像潜在梗表达")
    is_insider_knowledge: bool = Field(description="是否像圈内知识/圈层黑话")
    confidence: float = Field(ge=0.0, le=1.0, description="初步判定置信度")
    reason: str = Field(description="简短理由")
