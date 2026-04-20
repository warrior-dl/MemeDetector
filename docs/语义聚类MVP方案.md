# 语义聚类 MVP 方案

> 文档状态：草稿 v0.1
> 更新日期：2026-04-20
> 目标：在不动现有生产管线的前提下，跑通一个独立 MVP，验证「句法切分 + 新词发现 (PMI) + 火山 embedding + HDBSCAN 聚类」的组合是否能比当前 miner+researcher 显著降低误判率
> 范围：仅离线脚本，全部位于 `tools/embedding_cluster_mvp/`，不进生产代码路径

---

## 一、为什么要做这个 MVP

### 现状两个明确痛点（来自人工检查）

1. **小众知识误判**：单视频里被反复提及的某领域术语 / 知识点被当成梗。Agent 只能在「单视频评论 + 单次 web 搜索」窗口内判定，看不见跨视频复现度。
2. **口语化通用句式误判**：句式相似 / 常见名词被当成梗。Agent 没有「可溯源性」反向校验，也分不清「网上能搜到」与「在用户群里被复用」。

### 共同根因

当前判定是**孤岛式**——每个 bundle 在单视频小窗口内独立 adjudicate，**完全没有跨视频统计 / 传播信号**。学术上叫 meme tracking，参考 Leskovec et al. 2009《Meme-tracking and the dynamics of the news cycle》。

### 为什么是 embedding 聚类而不是别的

梗的典型形态是「**一族语义相近但字面不一致的变体**」，例如：

- 句式模仿：`今天也要XXX哦`、`XX の YY`
- 词汇替换：`绷不住了` → `蚌埠住了` → `蚌不住了`
- 逻辑改写：同一个梗的不同二创、扩写

字面 / 编辑距离 / Jaccard / BM25 都聚不起来这些变体；语义向量天然适合。
更关键：**梗的分布形状**（紧密小簇 + 跨视频 chunk）和「非梗」的分布形状（松散大簇 / 集中在单视频）有显著区分度，这本身就是强信号。

---

## 二、MVP 目标

### 必须达成

1. 跑通端到端：评论拉取 → 句法切分 → PMI 候选发现 → embedding → 聚类 → 候选打分 → 评估报告
2. 在 200 条人工标注集（100 真梗 + 100 非梗）上输出三项指标：comment-level **precision / recall / FP 构成**
3. 与现有 miner+researcher 在同样输入上的 P/R 做横向对比

### 明确不做

- 不改任何生产代码（`meme_detector/**` 不动）
- 不接入定时任务、不写 DB
- 不做梗溯源（用户当前只能标 is_meme 二分类）
- 不做线上化部署

### 成功判据

| 指标 | 目标 |
|---|---|
| precision（高分簇里真梗占比） | ≥ 0.75 |
| recall（gold 真梗里被聚到簇的占比） | ≥ 0.50 |
| FP 中「小众知识 + 通用口语」占比下降 | ≥ 50%（vs miner 当前 baseline） |
| 一次全量跑（10k 评论）耗时 | ≤ 10 分钟 |
| 一次全量跑火山 embedding 成本 | ≤ ¥5 |

任意一项不达标 → 调整方案而不是直接进生产。

---

## 三、核心 pipeline

```
Step 1  评论语料（DuckDB scout_raw_comments）
         │
Step 2  句法边界切分（按标点）           → 短句列表 A
         │
   并行：│
Step 3  整语料跑新词发现（PMI + 左右熵） → 候选梗串列表 P（全语料共用）
         │
Step 4  对每条评论：从短句 A 中捞命中候选串 P 的子段 → B
         │
Step 5  A ∪ B → 火山 embedding（bge-m3 / qwen3-embedding）
         │
Step 6  HDBSCAN（BERTopic 封装）聚类
         │
Step 7  簇打分（紧密度 + 跨视频数 + 跨作者数 + 通用口语词表命中）
         │
Step 8  高分簇 → 输出候选梗 + 典型变体 3 条
        低分簇 → 丢弃（小众知识 / 通用口语对应这里）
         │
Step 9  与 200 条 gold 标注集对账，输出 P/R/FP 报告
```

### 设计要点

#### 为什么用「句法切分」而不是滑窗

| 维度 | 滑窗（已否决） | 句法切分 + PMI |
|---|---|---|
| 变长梗（4-20 字） | 多尺度才能覆盖，参数难调 | 自然支持，无参数 |
| 句式模板梗（XX の YY） | 切到中间就被破坏 | 句法切分完整保留 |
| 小众知识拦截 | 无 | 左右熵 + 跨视频门槛 |
| 通用口语拦截 | 完全靠事后 HDBSCAN | PMI 预过滤 + HDBSCAN |
| 参数量 | 窗口大小 / 步长 | 几乎无（PMI 阈值有经验值） |

#### 为什么 PMI + 左右熵

- **PMI（互信息）**衡量 n-gram 内部字的凝固度。「绷不住了」凝固度高，「这个 up 主」凝固度低
- **左右熵**衡量这个 n-gram 在语料里左右上下文的多样性。真梗左右接的话题非常杂（被各种语境复用），左右熵高；小众知识 / 专业术语左右上下文非常单一（只在同类型视频出现），左右熵低
- 两者一起就能把「小众知识」和「真梗」分开——而单纯的频率统计做不到

---

## 四、目录结构

全部位于 `tools/embedding_cluster_mvp/`，不入生产 import 路径：

```
tools/embedding_cluster_mvp/
├── README.md                         # 怎么跑
├── pyproject.toml                    # 独立可选依赖组（jieba, hdbscan, bertopic, scikit-learn, pandas, httpx 已有）
├── data/
│   ├── gold.csv                      # 200 条人工标注（comment_id, text, is_meme）
│   └── stopwords_zh.txt              # 中文常见口语 / 停用词，PMI 拦截用
├── pipeline/
│   ├── __init__.py
│   ├── load_corpus.py                # SQL 拉评论
│   ├── sentence_split.py             # 句法切分
│   ├── new_word_discovery.py         # PMI + 左右熵
│   ├── embedding.py                  # 火山 API 调用
│   ├── clustering.py                 # BERTopic / HDBSCAN
│   ├── scoring.py                    # 簇打分
│   └── evaluation.py                 # 与 gold 对账
├── run_mvp.py                        # 一键跑全流程
└── outputs/                          # 跑完输出（gitignore）
    ├── candidates_pmi.csv            # 第 3 步输出
    ├── clusters.json                 # 第 6 步输出
    ├── ranked_clusters.json          # 第 7 步输出
    └── eval_report.md                # 第 9 步输出
```

---

## 五、各步骤设计

### Step 1 — 拉评论

直接读现有 DuckDB，限定到 scout 已抓取且 miner 已处理的视频范围。

**全量拉取，不加任何过滤条件**（最大化语料覆盖，让 PMI / 聚类在尽可能多的信号上工作）。

```python
# pipeline/load_corpus.py 伪代码
import duckdb
import pandas as pd

def load_corpus(db_path: str) -> pd.DataFrame:
    conn = duckdb.connect(db_path, read_only=True)
    rows = conn.execute("""
        SELECT
            c.comment_id,
            c.bvid,
            c.author_mid,
            c.content,
            c.publish_time
        FROM scout_raw_comments c
    """).fetchdf()
    conn.close()
    return rows
```

> **MVP 数据量**：当前 < 10k 条，全量跑；后期 < 1M，仍走全量，依赖下一步的长度过滤剔除极端值。

**轻量清洗**（不在 SQL 里做，留在下一步 sentence_split 里统一处理）：
- 空字符串 / 纯表情 / 长度 > 500 的评论在句法切分阶段自然被过滤（因为切不出合法短句）
- 重复评论去重放在 embedding 之前（按 `hash(content)` 去重，保留首条）

---

### Step 2 — 句法边界切分

```python
# pipeline/sentence_split.py 伪代码
import re

_SPLIT_PATTERN = re.compile(r"[，。！？~…\s,.!?]+")

def split_sentences(text: str, min_len: int = 3, max_len: int = 30) -> list[str]:
    parts = _SPLIT_PATTERN.split(text)
    return [p.strip() for p in parts if min_len <= len(p.strip()) <= max_len]
```

为什么 `min_len=3`：1-2 字的「啊」「哈哈」无意义。
为什么 `max_len=30`：超过 30 字基本是描述性长句而非梗，避免污染。

---

### Step 3 — 新词发现 PMI + 左右熵

整语料级别预跑一次，输出全局候选词表 `candidates_pmi.csv`。

```python
# pipeline/new_word_discovery.py 伪代码（约 80 行可实现）
from collections import Counter
import math

def discover_new_words(
    corpus: list[str],
    min_n: int = 2,
    max_n: int = 10,
    min_freq: int = 5,
    min_pmi: float = 4.0,
    min_entropy: float = 1.5,
) -> list[dict]:
    """
    返回：[{'term': '家人们谁懂啊', 'freq': 142, 'pmi': 9.2, 'left_entropy': 2.1, 'right_entropy': 2.4}, ...]
    """
    # 1) 统计所有 n-gram 频次
    ngram_freq = Counter()
    char_freq = Counter()
    left_ctx = {}  # term -> Counter(左字)
    right_ctx = {}  # term -> Counter(右字)
    for text in corpus:
        for ch in text:
            char_freq[ch] += 1
        for n in range(min_n, max_n + 1):
            for i in range(len(text) - n + 1):
                ng = text[i:i + n]
                ngram_freq[ng] += 1
                left = text[i - 1] if i > 0 else "^"
                right = text[i + n] if i + n < len(text) else "$"
                left_ctx.setdefault(ng, Counter())[left] += 1
                right_ctx.setdefault(ng, Counter())[right] += 1

    # 2) 计算 PMI 和左右熵
    total_chars = sum(char_freq.values())
    candidates = []
    for term, freq in ngram_freq.items():
        if freq < min_freq:
            continue
        # PMI = log(P(term)) - sum(log(P(char)))
        p_term = freq / total_chars
        p_chars = math.prod(char_freq[ch] / total_chars for ch in term)
        pmi = math.log(p_term / p_chars) if p_chars > 0 else 0
        if pmi < min_pmi:
            continue
        le = _entropy(left_ctx[term])
        re_ = _entropy(right_ctx[term])
        if min(le, re_) < min_entropy:
            continue
        candidates.append({
            'term': term, 'freq': freq, 'pmi': pmi,
            'left_entropy': le, 'right_entropy': re_,
        })

    # 3) 去包含关系（保留更长的，如果短的频次不显著高于长的）
    return _dedupe_by_inclusion(candidates)


def _entropy(counter: Counter) -> float:
    total = sum(counter.values())
    return -sum((c / total) * math.log(c / total) for c in counter.values() if c > 0)
```

**经验阈值**：`min_freq=5, min_pmi=4.0, min_entropy=1.5`，需要根据实际语料调
**计算量**：1 万条评论 + max_n=10，约 5 秒；100 万条约 5 分钟（PyPy 或 Cython 可加速）

---

### Step 4 — 评论 ↔ 候选串匹配

```python
# pipeline/sentence_split.py 续 伪代码
def extract_candidate_segments(text: str, candidate_set: set[str]) -> list[str]:
    """从评论里抽出命中候选串的子段"""
    return [c for c in candidate_set if c in text]
```

每条评论最终产出两组单元：句法短句 + 命中的 PMI 候选串。两组**一起**送 embedding。

---

### Step 5 — 火山 embedding

```python
# pipeline/embedding.py 伪代码
from meme_detector.http_client import get_async_client

# 复用 Batch B 的 client pool
async def embed_batch(texts: list[str], batch_size: int = 64) -> list[list[float]]:
    """火山 doubao-embedding-large-text 或 bge-m3"""
    client = get_async_client(_volcengine_embedding_profile())
    out: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        resp = await client.post(
            "https://ark.cn-beijing.volces.com/api/v3/embeddings",
            json={
                "model": settings.volcengine_embedding_model,  # 待配置
                "input": batch,
            },
            headers={"Authorization": f"Bearer {settings.volcengine_api_key}"},
        )
        resp.raise_for_status()
        data = resp.json()
        out.extend(item["embedding"] for item in data["data"])
    return out
```

**模型选型**：
- 首选 `doubao-embedding-large-text-240515`（火山自家，1024 维，中文优化）
- 备选 `bge-m3` 火山推理服务
- 不选 OpenAI（中文质量稍弱）

**成本估算**：
- 火山 embedding 计费：约 ¥0.0005 / 千 token
- 1 万条评论平均 30 字 = 30 万 token ≈ ¥0.15
- 100 万条评论 = 3000 万 token ≈ ¥15
- **MVP 一次跑完成本可忽略**

---

### Step 6 — 聚类

直接用 BERTopic（HDBSCAN + UMAP + c-TF-IDF 一体），自带处理流程。

```python
# pipeline/clustering.py 伪代码
from bertopic import BERTopic
from hdbscan import HDBSCAN
from umap import UMAP

def cluster(texts: list[str], embeddings: list[list[float]]):
    umap_model = UMAP(n_components=10, metric="cosine", min_dist=0.0, random_state=42)
    hdbscan_model = HDBSCAN(
        min_cluster_size=3,
        min_samples=2,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    topic_model = BERTopic(
        embedding_model=None,  # 已经有 embedding，跳过
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        calculate_probabilities=False,
        verbose=True,
    )
    topics, _ = topic_model.fit_transform(texts, embeddings=np.array(embeddings))
    return topic_model, topics
```

**为什么 BERTopic**：
- 把 UMAP 降维 → HDBSCAN 聚类 → c-TF-IDF 关键词抽取 一体化封装
- 直接输出每个簇的代表词（"梗本身"自然浮现）
- 中文社区有验证

**关键参数**：
- `min_cluster_size=3`：少于 3 条不算簇（噪点丢弃）
- `min_samples=2`：簇核心密度门槛
- `n_components=10`：1024 维 → 10 维降维（HDBSCAN 在高维下退化）

---

### Step 7 — 簇打分

```python
# pipeline/scoring.py 伪代码
def score_cluster(cluster: dict, video_index: dict, stopwords: set[str]) -> dict:
    """
    cluster = {'cluster_id': int, 'segments': [{text, comment_id, bvid, author}]}
    """
    n_videos = len({s['bvid'] for s in cluster['segments']})
    n_authors = len({s['author'] for s in cluster['segments']})
    intra_cohesion = _avg_pairwise_cosine(cluster['embeddings'])
    is_stopword_dominant = _check_stopword_overlap(cluster['top_words'], stopwords)

    score = (
        0.30 * min(n_videos / 10, 1.0) +     # 跨视频
        0.20 * min(n_authors / 20, 1.0) +    # 跨作者
        0.30 * intra_cohesion +              # 紧密度
        0.20 * (0.0 if is_stopword_dominant else 1.0)  # 通用口语惩罚
    )
    return {**cluster, 'score': score, 'n_videos': n_videos, 'n_authors': n_authors,
            'cohesion': intra_cohesion, 'stopword_dominant': is_stopword_dominant}
```

**阈值切档**（经验值，需要根据 gold 调）：
- `score ≥ 0.7`：高置信度梗候选
- `0.4 ≤ score < 0.7`：中等，需要 researcher 复核
- `score < 0.4`：丢弃

---

### Step 8 — 输出候选

每个高分簇输出一条记录：

```json
{
  "cluster_id": 17,
  "score": 0.82,
  "n_videos": 23,
  "n_authors": 41,
  "cohesion": 0.78,
  "top_terms_by_ctfidf": ["家人们谁懂啊", "家人们", "懂啊"],
  "representative_segments": [
    {"text": "家人们谁懂啊", "comment_id": "c_001", "bvid": "BV1xx"},
    {"text": "家人们谁懂啊真的", "comment_id": "c_023", "bvid": "BV2xx"},
    {"text": "家人们谁懂啊我哭了", "comment_id": "c_077", "bvid": "BV3xx"}
  ]
}
```

---

### Step 9 — 评估报告

```python
# pipeline/evaluation.py 伪代码
def evaluate(gold: pd.DataFrame, predictions: pd.DataFrame) -> dict:
    """
    gold: comment_id, is_meme (0/1)
    predictions: comment_id, predicted_meme (0/1)，从聚类输出反推
                 (任何评论的句法短句或 PMI 候选段落入了高分簇 → 1)
    """
    merged = gold.merge(predictions, on='comment_id', how='left').fillna(0)
    tp = ((merged.is_meme == 1) & (merged.predicted_meme == 1)).sum()
    fp = ((merged.is_meme == 0) & (merged.predicted_meme == 1)).sum()
    fn = ((merged.is_meme == 1) & (merged.predicted_meme == 0)).sum()
    tn = ((merged.is_meme == 0) & (merged.predicted_meme == 0)).sum()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    fp_breakdown = analyze_fp_categories(merged[(merged.is_meme == 0) & (merged.predicted_meme == 1)])

    return {
        'precision': precision, 'recall': recall,
        'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn,
        'fp_breakdown': fp_breakdown,  # 小众知识 / 通用口语 / 其他
    }
```

输出 `outputs/eval_report.md`：

```markdown
# Eval Report 2026-04-20

## 数据
- Gold: 200 条 (100 真梗 / 100 非梗)
- 评估时刻聚类簇数: 47
- 高分簇 (score ≥ 0.7): 12

## 指标
| 项 | MVP | miner+researcher baseline |
|---|---|---|
| Precision | 0.78 | 0.55 |
| Recall    | 0.62 | 0.71 |
| F1        | 0.69 | 0.62 |

## FP 构成
| 类别 | MVP 数量 | baseline 数量 |
|---|---|---|
| 小众知识 | 3 | 18 |
| 通用口语 | 5 | 22 |
| 其他    | 2 | 5  |
```

---

## 六、人工标注规范

### 数据来源

从 `scout_raw_comments` 随机抽 400 条，去重后取前 200，覆盖至少 10 个不同视频。

### 标注格式（CSV）

```csv
comment_id,bvid,text,is_meme,note
c_001,BV1abc,"家人们谁懂啊太对了",1,经典梗
c_002,BV2def,"视频不错 up 主加油",0,普通评论
c_003,BV3ghi,"这个文物是宋代的",0,小众知识/专业知识
c_004,BV4jkl,"哈哈哈哈哈",0,通用口语
c_005,BV5mno,"绷不住了笑死",1,梗
```

### 标注原则

1. **宁可错杀通用口语为 0，不要误收为 1**（项目当前痛点是过收）
2. 含梗（哪怕夹杂其他内容）→ 标 1
3. 小众专业名词 / 知识点 → 标 0（即使是某圈层"暗号"）
4. 边界情况优先标 0
5. `note` 字段记录类别，方便事后统计 FP 构成

### 时长

预计 40-60 分钟。

---

## 七、依赖与环境

新增依赖（**全部隔离在 `tools/embedding_cluster_mvp/pyproject.toml`，不污染主项目**）：

```toml
[project]
name = "meme-cluster-mvp"
version = "0.1.0"
dependencies = [
    "duckdb>=1.0",
    "pandas>=2.0",
    "numpy>=1.24",
    "scikit-learn>=1.3",
    "umap-learn>=0.5.5",
    "hdbscan>=0.8.33",
    "bertopic>=0.16",
    "httpx>=0.27",
    "tqdm>=4.66",
]
```

**不需要本地 GPU**——embedding 走火山 API。

---

## 八、运行步骤

```bash
cd tools/embedding_cluster_mvp
pip install -e .

# 1) 准备 gold 标注（人工）
vim data/gold.csv

# 2) 一键跑
python run_mvp.py --db ../../data/memes.duckdb --days 30 \
    --out outputs/run-$(date +%Y%m%d)

# 3) 看报告
cat outputs/run-*/eval_report.md
```

---

## 九、风险与备选方案

| 风险 | 概率 | 应对 |
|---|---|---|
| 火山 embedding 在中文短文本质量不达预期 | 中 | 备选 bge-m3 本地；或试 jina-embeddings-v3 / qwen3-embedding |
| 10k 评论数据量太小，PMI 统计不显著 | 高 | 降低 `min_freq=3`；或合并多月数据 |
| 200 gold 样本量小，指标方差大 | 中 | 跑 5 次取均值；分层抽样 |
| HDBSCAN 在 10k 点上仍然把所有梗判为噪声 | 低 | 调小 `min_cluster_size=2`；UMAP 降维到 5 维 |
| BERTopic 包过重 / 安装慢 | 低 | 自己实现 UMAP + HDBSCAN + c-TF-IDF（约 100 行） |

---

## 十、与生产管线的关系

**MVP 阶段：完全独立**，不动 `meme_detector/**` 任何代码。

**MVP 跑通后**（如果指标达标），后续 PR（**不在本文档范围**）的可能改造：

1. 把 `tools/embedding_cluster_mvp/pipeline/` 的稳定模块迁移到 `meme_detector/clustering/`
2. 新增 `meme_candidates` 表（候选注册表）
3. miner 改名为 candidate extractor，输出粒度变成 (term, bvid, comment_id, embedding)
4. 周期性聚类 job → 高分候选触发 researcher
5. researcher 不变，但触发条件从「单视频 bundle」改为「跨视频候选簇」

这些改造都是后话，**MVP 阶段一律不做**。

---

## 十一、待用户确认的事项

| # | 问题 | 状态 |
|---|---|---|
| 1 | embedding 模型最终用 `doubao-embedding-large-text-240515` 还是 `bge-m3`？（看你火山控制台开通了哪个） | 待确认 |
| 2 | 数据范围 | ✅ 已确认：**全量拉取，不限定条件** |
| 3 | gold 200 条人工标注由你来做，**预计什么时候能给到 `data/gold.csv`**？这个不给，MVP 没法评估 | 待确认 |
| 4 | 是否同意「MVP 阶段完全独立、不动生产代码」这个边界？ | 待确认 |

回完剩余 3 项即可开始实现 MVP（**实现属于另一个 PR / 另一个工单**）。本文档只负责说清「我们要做什么、怎么做、为什么这么做」。
