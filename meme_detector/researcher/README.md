# researcher — 评论证据包裁决模块

负责消费 Miner 产出的评论证据包，比较多个 competing hypotheses，输出最终 `ResearchDecision`。只有在 `accept` 或 `rewrite_title` 时，才生成结构化 `MemeRecord` 并写入梗库。

## 文件

| 文件 | 职责 |
|------|------|
| `models.py` | `MemeRecord`、`ResearchRunResult` 等结果模型 |
| `tools.py` | 工具函数：火山引擎联网搜索、URL 真实性验证 |
| `decider.py` | 新主流程：基于 bundle 做 hypothesis 裁决 |
| `persistence.py` | Research 阶段 bundle 读取、裁决写回、梗库入库封装 |
| `agent.py` | Research 编排层，对外稳定入口 `run_research()` |

## 触发方式

- **自动**：每周一 06:00 由 `scheduler.py` 调用 `run_research()`
- **手动**：`python -m meme_detector research`

当前流程不会自动触发 `miner`。
如果还存在 `scout_raw_videos.miner_status='pending'` 的视频，`research` 会直接退出并提示先手动运行 `miner`。
如果 `serve` 正在运行，推荐直接在根路径工作台 `/` 中触发任务，避免 DuckDB 锁冲突。

## 当前主流程

```
前置条件：不存在待 Miner 处理的视频
    ↓
读取全部 queued 的 comment bundle（最多 AI_BATCH_SIZE 个）
    ↓
Research LLM 基于 bundle 做 hypothesis 裁决
    ↓
输出 ResearchDecision：
    accept / reject / rewrite_title / manual_review / merge_into_existing
    ↓
若有 record，则验证 source_urls
    ↓
写入 research_decisions
    ↓
需要入库时同步写入：
    Meilisearch + DuckDB meme_records
```

说明：
- 当前实现只保留 `bundle -> decision -> record` 主链路
- 旧的 `candidate/word` 预筛与深挖模块已删除

`run_research()` 返回：

```python
ResearchRunResult(
    pending_count=...,
    adjudicated_count=...,
    accepted_count=...,
    rejected_count=...,
    accepted_records=[...],
    rejected_bundle_ids=[...],
    failed_bundle_ids=[...],
    blocked_pending_video_count=...,
)
```

## 依赖配置

| 环境变量 | 用途 | 必填 |
|----------|------|------|
| `LLM_API_KEY` | 默认 OpenAI-compatible API 密钥 | ✅ |
| `LLM_BASE_URL` | 默认接口地址 | ✅ |
| `LLM_MODEL` | 默认模型名 | ✅ |
| `LLM_PROVIDER` | `auto/openai/deepseek/moonshotai` | 可选 |
| `RESEARCH_LLM_API_KEY` | Research 专属密钥 | 可选 |
| `RESEARCH_LLM_BASE_URL` | Research 专属接口地址 | 可选 |
| `RESEARCH_LLM_MODEL` | Research 专属模型名 | 可选 |
| `RESEARCH_LLM_PROVIDER` | Research 专属 provider 提示 | 可选 |
| `WEB_SEARCH_API_KEY` | 火山引擎联网搜索 API Key | 可选（缺失则跳过 Web 搜索） |
| `BIBIGPT_API_TOKEN` | BibiGPT 视频总结 API | 可选（通常由 Miner 消费，缺失则仅保留基础元数据） |

## 幻觉防护

1. **Bundle 裁决**：Research 直接比较多个 hypothesis，不再默认输入词就是梗本体
2. **URL 验证**：HTTP HEAD 检查来源真实性，过滤 AI 编造的链接
3. **人工兜底**：所有入库记录 `human_verified=false`，可通过 API 端点复核
4. **结构收敛**：Research 仅以评论证据包为输入，不再维护候选词分支

## 扩展：接入其他 LLM

统一入口在 `meme_detector.llm_factory`：

- 默认读取 `LLM_*`
- `Researcher` 可通过 `RESEARCH_LLM_*` 单独覆盖
- `LLM_PROVIDER/RESEARCH_LLM_PROVIDER` 支持 `auto/openai/deepseek/moonshotai`

如果目标服务是标准 OpenAI-compatible 接口，通常只需要改 `*_LLM_BASE_URL` 和 `*_LLM_MODEL`。
只有遇到特定 provider 的工具 / schema 兼容性差异时，才需要显式指定 `*_LLM_PROVIDER`。
