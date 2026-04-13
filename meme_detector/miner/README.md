# miner — 评论线索与证据包双阶段模块

负责消费 Scout 原始视频快照，拆成两个独立阶段：
- Stage 1：调用 BibiGPT 获取视频内容上下文，对每条评论做“是否值得侦查”的初步判定
- Stage 2：只消费高价值评论，对其执行片段拆解、选择性搜索和评论证据包生成

## 文件

| 文件 | 职责 |
|------|------|
| `models.py` | Stage 1 / Stage 2 运行结果模型 |
| `video_context.py` | B站视频内容解析、BibiGPT 调用与 DuckDB 缓存 |
| `analysis.py` | 评论批处理、LLM 请求、fallback、Agent 对话落库 |
| `bundler.py` | 高价值评论的 span / hypothesis / evidence 生成 |
| `persistence.py` | Stage 1 / Stage 2 持久化与状态流转 |
| `scorer.py` | Miner 双阶段编排层 |

## 触发方式

- 自动：
  - 每日 03:00 运行 `run_miner_insights()`
  - 每日 03:20 运行 `run_miner_bundles()`
- 手动：
  - `python -m meme_detector miner_insights`
  - `python -m meme_detector miner_bundles`
  - `python -m meme_detector miner` 可串行执行两个阶段

如果 `serve` 正在运行，推荐直接在根路径工作台 `/` 中触发 Miner，而不是另开 CLI 进程，避免 DuckDB 锁冲突。

## 输入与输出

输入：
- 标题
- 简介
- 标签
- 视频内容摘要 / 正文 / 字幕摘录
- 评论列表

输出：
- `miner_comment_insights`
  - `is_meme_candidate`
  - `is_insider_knowledge`
  - `confidence`
  - `reason`
  - `video_context`
  - `status=pending_bundle / bundling / bundled / bundle_failed / discarded`
- `comment_insights`
- `comment_spans`
- `hypotheses`
- `hypothesis_spans`
- `evidences`

运行结果：

```python
MinerInsightsRunResult(
    target_date="YYYY-MM-DD",
    video_count=...,
    insight_count=...,
    high_value_count=...,
)

MinerBundlesRunResult(
    target_date="YYYY-MM-DD",
    queued_insight_count=...,
    bundled_count=...,
    failed_insight_count=...,
)
```

## 处理流程

```
Stage 1:
读取全部 pending 的 scout_raw_videos
    ↓
逐视频获取视频上下文（带 DuckDB 缓存）
    ↓
按评论批次调用模型打分
    ↓
写入 miner_comment_insights
    ├─▶ 高价值评论 → status=pending_bundle
    └─▶ 普通评论   → status=discarded
    ↓
当前视频标记为 mined

Stage 2:
读取 status in (pending_bundle, bundle_failed) 的 miner_comment_insights
    ↓
逐条生成 comment bundle
    ↓
写入 comment_insights / spans / hypotheses / evidences
    ├─▶ 成功 → insight.status=bundled
    └─▶ 失败 → insight.status=bundle_failed
```

Stage 1 按视频增量落库，Stage 2 按评论线索增量落库。

## 关键配置

| 环境变量 | 说明 |
|----------|------|
| `LLM_API_KEY` | 默认 OpenAI-compatible 模型配置 |
| `LLM_BASE_URL` | 默认模型接口地址 |
| `LLM_MODEL` | 默认模型名 |
| `LLM_PROVIDER` | `auto/openai/deepseek/moonshotai`，默认 `auto` |
| `MINER_LLM_API_KEY` | Miner 专属模型密钥，留空则继承 `LLM_API_KEY` |
| `MINER_LLM_BASE_URL` | Miner 专属接口地址，留空则继承 `LLM_BASE_URL` |
| `MINER_LLM_MODEL` | Miner 专属模型名，留空则继承 `LLM_MODEL` |
| `MINER_LLM_PROVIDER` | Miner 专属 provider 提示，留空则继承 `LLM_PROVIDER` |
| `BIBIGPT_API_TOKEN` | 视频内容解析 |
| `MINER_COMMENT_CONFIDENCE_THRESHOLD` | 进入 bundle 生成阶段的最低线索阈值 |
| `MINER_COMMENTS_BATCH_SIZE` | 单次送模型的评论批大小 |
| `MINER_LLM_TIMEOUT_SECONDS` | 单次模型请求超时 |
| `MINER_LLM_MAX_RETRIES` | OpenAI 客户端级别重试次数 |
| `WEB_SEARCH_API_KEY` | Miner 第二阶段搜索取证 |
