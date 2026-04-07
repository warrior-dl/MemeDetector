# miner — 评论线索初筛模块

负责消费 Scout 原始视频快照，调用 BibiGPT 获取视频内容上下文，再把标题、简介、标签、视频内容和评论一起交给模型，对每条评论做“潜在梗 / 圈内知识”初步判定。

## 文件

| 文件 | 职责 |
|------|------|
| `models.py` | `CommentInsightResult` 与 `MinerRunResult` |
| `video_context.py` | B站视频内容解析、BibiGPT 调用与 DuckDB 缓存 |
| `analysis.py` | 评论批处理、LLM 请求、fallback、Agent 对话落库 |
| `persistence.py` | 待处理视频读取、线索写库、按视频标记 mined |
| `scorer.py` | Miner 编排层，逐视频处理并统计结果 |

## 触发方式

- 自动：每日 03:00 由 `scheduler.py` 调用 `run_miner()`
- 手动：`python -m meme_detector miner`

如果 `serve` 正在运行，推荐直接在根路径工作台 `/` 中触发 Miner，而不是另开 CLI 进程，避免 DuckDB 锁冲突。

## 输入与输出

输入：
- 标题
- 简介
- 标签
- 视频内容摘要 / 正文 / 字幕摘录
- 评论列表

输出：`miner_comment_insights`
- `is_meme_candidate`
- `is_insider_knowledge`
- `confidence`
- `reason`
- `video_context`

运行结果：

```python
MinerRunResult(
    target_date="YYYY-MM-DD",
    video_count=...,
    insight_count=...,
    high_value_count=...,
)
```

## 处理流程

```
读取全部 pending 的 scout_raw_videos
    ↓
逐视频获取视频上下文（带 DuckDB 缓存）
    ↓
按评论批次调用模型打分
    ↓
写入 miner_comment_insights
    ↓
当前视频立即标记为 mined
```

Miner 现在是按视频增量落库的。
即使中途失败，已经处理完成的视频也不会在下次重跑时重复计算。

## 关键配置

| 环境变量 | 说明 |
|----------|------|
| `DEEPSEEK_API_KEY` | 评论初筛模型 |
| `BIBIGPT_API_TOKEN` | 视频内容解析 |
| `MINER_COMMENT_CONFIDENCE_THRESHOLD` | 进入 Researcher 的最低线索阈值 |
| `MINER_COMMENTS_BATCH_SIZE` | 单次送模型的评论批大小 |
| `MINER_LLM_TIMEOUT_SECONDS` | 单次模型请求超时 |
| `MINER_LLM_MAX_RETRIES` | OpenAI 客户端级别重试次数 |
