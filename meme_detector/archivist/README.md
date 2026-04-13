# archivist — 存储层

管理系统的两个持久化后端：运行审计数据库（DuckDB）和梗库检索引擎（Meilisearch）。

## 文件

| 文件 | 职责 |
|------|------|
| `duckdb_store.py`  | 评论证据包、运行记录、视频上下文缓存、Agent 对话落盘 |
| `meili_store.py`   | MemeRecord 写入/更新、全文检索、索引初始化 |

## 两个后端的职责边界

```
DuckDB（嵌入式文件数据库）         Meilisearch（全文搜索引擎）
─────────────────────────────     ─────────────────────────────
Source of Truth                   查询加速层
存储原始评论/视频快照                存储 AI 确认的 MemeRecord
存储评论证据包与裁决过程              支持模糊搜索、过滤、排序
pipeline_runs / agent_conversations
保存调度与 Agent 审计日志
video_context_cache
缓存视频背景分析结果
数据可重放（Meilisearch 可重建）    不存原始采集与裁决中间态
```

## DuckDB 表结构

```sql
scout_raw_videos    -- Scout 采集到的视频快照
  bvid, collected_date, partition, title, comments_json, research_status, miner_status, ...

scout_raw_comments  -- 结构化评论快照
  rpid, bvid, collected_date, message, content_json, raw_reply_json, ...

media_assets / comment_media_links -- 评论图片资产与关联关系

comment_insights -- 评论证据包入口
  bundle_id, insight_id, bvid, comment_text, worth_investigating, signal_score, ...

comment_spans / hypotheses / hypothesis_spans / evidences / research_decisions
  评论切分结果、竞争假设、证据与最终裁决

meme_records    -- AI 确认的词条备份（Meilisearch 的镜像）
  id, title, alias, definition, origin, ...

pipeline_runs   -- Scout / Miner / Researcher 任务运行记录
  id, job_name, trigger_mode, status, started_at, finished_at, payload_json, ...

video_context_cache  -- 视频内容背景缓存（BibiGPT + 元信息）
  bvid, video_url, title, status, duration_seconds, summary, content_text, ...

miner_comment_insights  -- Miner 对评论的初步判定结果
  insight_id, bvid, comment_text, confidence, is_meme_candidate, ...

agent_conversations  -- Researcher 单词条 Agent 对话审计
  id, run_id, word, status, messages_json, output_json, error_message, ...
```

`video_context_cache.status` 常见值：

- `ready`：已获取视频背景，可复用
- `skipped`：超过时长限制等原因跳过
- `unavailable`：外部视频背景服务未启用

`agent_conversations.status` 常见值：

- `running`：已创建对话记录但尚未结束
- `success`：对话成功完成并产出结构化结果
- `failed`：对话失败，已保存错误信息和已产生的消息

## Meilisearch 索引配置

| 配置项 | 字段 |
|--------|------|
| 可搜索 | `title`, `alias`, `definition`, `origin` |
| 可过滤 | `category`, `platform`, `lifecycle_stage`, `human_verified` |
| 可排序 | `heat_index`, `updated_at`, `first_detected_at`, `confidence_score` |

## 重建索引

Meilisearch 的数据可以随时从 DuckDB 重建：
1. 清空 Meilisearch 索引
2. 查询 DuckDB `meme_records` 表
3. 批量 `upsert_meme()` 写回

## 当前缓存与审计策略

- 视频背景分析结果先查 `video_context_cache`，命中后不重复请求外部 API
- 超过 15 分钟的视频会标记为 `skipped` 并缓存跳过原因
- Researcher 对每个评论证据包的完整上下文会写入 `agent_conversations`
- Pipeline 任务执行摘要写入 `pipeline_runs`，便于管理台展示
- Miner 评论打分结果写入 `miner_comment_insights`，便于重复调参和重跑

数据文件位置：`data/duckdb/freq.db`
