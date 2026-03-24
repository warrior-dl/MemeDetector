# archivist — 存储层

管理系统的两个持久化后端：词频时序数据库（DuckDB）和梗库检索引擎（Meilisearch）。

## 文件

| 文件 | 职责 |
|------|------|
| `duckdb_store.py`  | 词频时序写入、候选词队列管理、Score 计算 SQL |
| `meili_store.py`   | MemeRecord 写入/更新、全文检索、索引初始化 |

## 两个后端的职责边界

```
DuckDB（嵌入式文件数据库）         Meilisearch（全文搜索引擎）
─────────────────────────────     ─────────────────────────────
Source of Truth                   查询加速层
存储原始词频时序数据                存储 AI 确认的 MemeRecord
candidates 候选词队列              支持模糊搜索、过滤、排序
支持 SQL 窗口函数计算 Score         支持 < 50ms 极速检索
数据可重放（Meilisearch 可重建）    不存时序/原始数据
```

## DuckDB 表结构

```sql
word_freq       -- 每日词频快照（核心时序数据）
  word, date, partition, freq, doc_count

candidates      -- 待 AI 审核的候选词队列
  word, score, is_new_word, sample_comments, detected_at, status

meme_records    -- AI 确认的词条备份（Meilisearch 的镜像）
  id, title, alias, definition, origin, ...
```

`status` 字段流转：`pending` → `accepted`（入库）/ `rejected`（排除）

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

数据文件位置：`data/duckdb/freq.db`（不要 gitignore 生产数据库）
