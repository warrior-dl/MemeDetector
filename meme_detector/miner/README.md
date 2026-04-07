# miner — 评论线索初筛模块

负责消费 Scout 原始视频快照，调用 BibiGPT 获取视频内容上下文，再把标题、简介、标签、视频内容和评论一起交给模型，对每条评论做“潜在梗 / 圈内知识”初步判定。

## 文件

| 文件 | 职责 |
|------|------|
| `models.py` | `CommentInsightResult` 结构化输出模型 |
| `video_context.py` | B站视频内容解析、BibiGPT 调用与 DuckDB 缓存 |
| `scorer.py` | Miner 主流程：批量读取 Scout 快照并写入评论线索 |

## 触发方式

- 自动：每日 03:00 由 `scheduler.py` 调用 `run_miner()`
- 手动：`python -m meme_detector miner`

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

## 关键配置

| 环境变量 | 说明 |
|----------|------|
| `DEEPSEEK_API_KEY` | 评论初筛模型 |
| `BIBIGPT_API_TOKEN` | 视频内容解析 |
| `MINER_COMMENT_CONFIDENCE_THRESHOLD` | 进入 Researcher 的最低线索阈值 |
| `MINER_COMMENTS_BATCH_SIZE` | 单次送模型的评论批大小 |
