# embedding_cluster_mvp

语义聚类 MVP 配套工具目录。完全独立于 `meme_detector/` 生产代码。

> 设计文档见 [docs/语义聚类MVP方案.md](../../docs/语义聚类MVP方案.md)

---

## 当前包含

### `annotate.py` — Gold 标注工具

从 DuckDB 的 `scout_raw_comments` 表随机抽样评论，让你在命令行做 `is_meme` 二分类标注，产出 `data/gold.csv` 作为 MVP 评估集（目标 200 条：100 正 + 100 负）。

**特性**：
- 按 `--seed` 做确定性抽样，重复运行顺序稳定
- 每标一条立即 `flush` 到 CSV，异常退出不丢数据
- 自动断点续标（读已有 CSV，按 `(rpid, bvid)` 跳过已标）
- 支持 `b` 撤销上一条、`s` 跳过、`q` 中途退出
- 纯 stdlib + `duckdb`，无 GUI 依赖

**使用**：

```bash
# 默认参数（目标 200 条，种子 42）
python tools/embedding_cluster_mvp/annotate.py

# 指定 DB 和输出路径
python tools/embedding_cluster_mvp/annotate.py \
    --db data/duckdb/freq.db \
    --out tools/embedding_cluster_mvp/data/gold.csv \
    --n 200 --seed 42
```

**交互**：

```
======================================================================
进度：12/200   池位置：15
BV  : BV1xxxxxxxx
RPID: 2523612345
UP  : 某 UP 主 (mid=12345678)
----------------------------------------------------------------------
家人们谁懂啊，这视频我看了三遍
======================================================================
[1=梗 0=非 s=跳 b=撤 q=退 ?=帮助] > 1
备注（回车跳过）> 经典梗
```

**CSV 格式**（`data/gold.csv`）：

| 列 | 说明 |
|---|---|
| `comment_id` | 评论 `rpid`（字符串化） |
| `bvid` | 视频 BV 号 |
| `mid` | 评论作者 mid |
| `uname` | 评论作者昵称 |
| `text` | 评论原文 |
| `is_meme` | `1` = 真梗 / `0` = 非梗 |
| `note` | 你的备注（可空） |
| `labeled_at` | 标注时间（UTC ISO8601） |

**标注原则**（对齐 MVP 评估痛点）：

1. 宁可错杀通用口语为 `0`，不要误收为 `1`（当前项目痛点是过收）
2. 评论含梗（哪怕夹杂其他内容）→ `1`
3. 小众专业名词 / 知识点 → `0`（即使是某圈层"暗号"）
4. 边界情况优先 `0`
5. `note` 字段记录类别（如 `小众知识` / `通用口语`），方便事后统计 FP 构成

---

## 计划中（未实现）

| 脚本 | 用途 |
|---|---|
| `pipeline/load_corpus.py` | 拉全量评论 |
| `pipeline/sentence_split.py` | 句法边界切分 |
| `pipeline/new_word_discovery.py` | PMI + 左右熵新词发现 |
| `pipeline/embedding.py` | 火山 doubao embedding 调用 |
| `pipeline/clustering.py` | BERTopic / HDBSCAN 聚类 |
| `pipeline/scoring.py` | 簇打分（跨视频 / 跨作者 / 紧密度） |
| `pipeline/evaluation.py` | 对 `gold.csv` 算 P/R/FP |
| `run_mvp.py` | 一键跑全流程 |

以上属于 MVP 实现工单范围，本 PR 不包含。
