"""
核心数据模型。
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class MemeRecord(BaseModel):
    """梗的完整记录，同时作为 Meilisearch 文档和 AI 输出模型。"""

    id: str = Field(description="词本身，作为唯一主键")
    title: str = Field(description="标准写法/展示标题")
    alias: list[str] = Field(default_factory=list, description="别称、变体写法")
    definition: str = Field(description="梗的含义解释，100字以内，通俗易懂")
    origin: str = Field(description="梗的来源/起源，说明在哪个视频/事件中首次流行")
    category: list[str] = Field(
        description="分类标签，从以下选择：抽象、谐音、游戏、影视、音乐、社会现象、二次元、其他"
    )
    platform: str = Field(default="Bilibili", description="主要流行平台")
    heat_index: int = Field(ge=0, le=100, description="热度指数 0-100")
    lifecycle_stage: Literal["emerging", "peak", "declining"] = Field(
        description="生命周期阶段：emerging=新兴, peak=高峰, declining=衰退"
    )
    first_detected_at: date = Field(description="首次检测到的日期")
    source_urls: list[str] = Field(default_factory=list, description="溯源链接列表")
    confidence_score: float = Field(ge=0.0, le=1.0, description="AI 判定置信度")
    human_verified: bool = Field(default=False, description="是否经过人工验证")
    updated_at: date = Field(description="最后更新日期")


class QuickScreenResult(BaseModel):
    """Step1 快速筛选的输出模型。"""

    word: str
    is_meme: bool = Field(description="是否判定为网络梗/亚文化词汇")
    confidence: float = Field(ge=0.0, le=1.0, description="置信度")
    candidate_category: str = Field(description="初步分类猜测")
    reason: str = Field(description="简短理由（1-2句）")


class CandidateSeed(BaseModel):
    """Researcher 基于 Scout 原始评论归纳出的候选词。"""

    word: str = Field(description="候选词或短语")
    confidence: float = Field(ge=0.0, le=1.0, description="提取置信度")
    reason: str = Field(description="为何值得进入候选队列")
    related_bvids: list[str] = Field(default_factory=list, description="相关视频 BV 号")
    sample_comments: list[str] = Field(default_factory=list, description="代表性评论")
