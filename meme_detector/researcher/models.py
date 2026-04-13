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


class ResearchAcceptedRecord(BaseModel):
    """Research 成功入库后的结果摘要。"""

    id: str
    title: str
    heat_index: int = Field(ge=0, le=100)
    lifecycle_stage: Literal["emerging", "peak", "declining"]
    confidence_score: float = Field(ge=0.0, le=1.0)


class ResearchRunResult(BaseModel):
    """Research 流程运行结果。"""

    pending_count: int = 0
    adjudicated_count: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    accepted_records: list[ResearchAcceptedRecord] = Field(default_factory=list)
    rejected_bundle_ids: list[str] = Field(default_factory=list)
    failed_bundle_ids: list[str] = Field(default_factory=list)
    blocked_pending_video_count: int = 0

    def __getitem__(self, key: str):
        return getattr(self, key)

    def get(self, key: str, default=None):
        return getattr(self, key, default)

    @classmethod
    def blocked_by_pending_videos(cls, pending_video_count: int) -> "ResearchRunResult":
        return cls(blocked_pending_video_count=pending_video_count)

    def add_accepted_record(self, record: MemeRecord) -> None:
        self.accepted_records.append(
            ResearchAcceptedRecord(
                id=record.id,
                title=record.title,
                heat_index=record.heat_index,
                lifecycle_stage=record.lifecycle_stage,
                confidence_score=record.confidence_score,
            )
        )
        self.accepted_count = len(self.accepted_records)
