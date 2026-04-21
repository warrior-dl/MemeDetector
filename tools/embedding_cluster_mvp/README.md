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

## `pipeline_v2/` — Meme 知识图 + Leiden + LLM Judge（MVP v2）

v1（PMI + 嵌入 + HDBSCAN + 跨视频/跨作者打分）在 355 条评论上 TOP 5 全是 FP（平台词 / 通用套话）。根因是**打分方向错了**——高频 + 跨视频 + 跨作者天然奖励的就是"大会员 / 哔哩哔哩 / 只能说"。

v2 完全换思路（见设计文档 §MVP v2）：

```
Layer 1 [extractor.py]      LLM 对每条评论做类型化抽取，返回 {text, type ∈
                            {meme_candidate, platform_term, generic_phrase,
                             proper_noun}}。非 meme_candidate 直接丢弃
                            ← 这一步就消灭了 v1 TOP 5 的全部 FP

Layer 2 [graph_builder.py]  基于 LLM 抽取结果 + 候选词 doubao embedding，
                            用 networkx 构图：
                              节点：candidate / comment / video / author
                              边：  contains / posted_on / posted_by
                                    variant（余弦相似度≥阈值）
                                    co_occurs（同评论共现）

Layer 3 [community.py]      在 candidate 子图上跑 Leiden，算每个社区：
                              size / total_freq / n_videos / n_authors
                              internal_density / avg_variant_sim
                              cross_video_ratio / burst_score

Layer 4 [judge.py]          对每个社区做单轮 pairwise LLM judge，prompt
                            里同时给出已知正样本（"家人们谁懂啊"、
                            "绷不住"、"一眼 AI" …）+ 已知负样本
                            （"大会员"、"只能说"、"打 call" …），
                            让 LLM 对比判定 meme / not_meme / uncertain

Layer 5 [evaluation.py]     若 data/gold.csv 存在则算 comment-level
                            Precision / Recall / F1 + FP 构成
```

### 依赖

```bash
pip install -e '.[mvp]'   # networkx, python-igraph, leidenalg, numpy
```

并确保 `.env` 已配置：

- `MINER_LLM_API_KEY` / `MINER_LLM_MODEL`（或回退 `LLM_*`）供 extractor 用
- `RESEARCH_LLM_API_KEY` / `RESEARCH_LLM_MODEL` 供 judge 用（可以同一家）
- `EMBEDDING_API_KEY`（或 `ARK_API_KEY`）+ `EMBEDDING_MODEL=doubao-embedding-large-text-240515`

### 一键运行

```bash
python -m tools.embedding_cluster_mvp.pipeline_v2.run \
  --db data/duckdb/freq.db \
  --out-dir tools/embedding_cluster_mvp/data/v2_run_001
```

或从仓库根直接：

```bash
python tools/embedding_cluster_mvp/pipeline_v2/run.py \
  --db data/duckdb/freq.db \
  --out-dir tools/embedding_cluster_mvp/data/v2_run_001
```

小规模先试水：

```bash
python tools/embedding_cluster_mvp/pipeline_v2/run.py --limit 50
```

### 产出（在 `--out-dir` 下）

| 文件 | 说明 |
|---|---|
| `extracted.jsonl`    | Layer 1 LLM 抽取缓存（重跑时自动复用，不再烧 API） |
| `embeddings.jsonl`   | Layer 1.5 候选词 embedding 缓存 |
| `graph.gexf`         | Layer 2 图（可导入 Gephi / Cytoscape 可视化） |
| `communities.json`   | Layer 3-4 社区 + 裁决明细 |
| `eval_report.md`     | Layer 5 评估报告（即使无 gold 也会输出结构汇总） |
| `run_meta.json`      | 本次运行参数 / 统计 |

### 主要可调参数

| CLI flag | 默认 | 作用 |
|---|---|---|
| `--variant-threshold` | 0.82 | candidate 间建 variant 边的余弦相似度阈值。降 → 社区更大更粗；升 → 社区更细更纯 |
| `--leiden-resolution` | 1.0  | Leiden 分辨率。>1 → 社区更小；<1 → 社区更大 |
| `--min-freq`          | 1    | candidate 最小出现频次（小语料先设 1） |
| `--extract-target`    | miner | extractor 用哪个 LLM（default/miner/research） |
| `--judge-target`      | research | judge 用哪个 LLM |

### 和 v1 的关系

- `pipeline_v2/` 和 v1 的 `pipeline/`（如果以后生成）**完全并列**、不互相调用
- 都读同一个 `scout_raw_comments`、都写到各自 `--out-dir`
- 都产出 `eval_report.md`，格式对齐方便 diff

---

### 单元测试（纯 pure-logic，不依赖 LLM）

```bash
python -m pytest tools/embedding_cluster_mvp/tests/ -v
```

覆盖：JSON 解析、候选聚合、图结构、variant 阈值、Leiden 分离、P/R/F1。
