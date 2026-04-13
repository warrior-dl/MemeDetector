# MemeDetector MVP 设计方案 v1.0

> 文档状态：草稿
> 更新日期：2026-03-24
> 定位：面向开发团队的可执行 MVP 规格文档
>
> 说明：本文档中的 Research 流程仍以“候选词”为中心。自 2026-04-09 起，新的实现方案已改为“评论证据包 + hypothesis 裁决”架构，详见 [评论证据包重构方案](./评论证据包重构方案.md)。

---

## 一、MVP 目标与范围

### 核心目标
在 **4 周内**交付一个可运行的端到端 pipeline：
- 每天自动采集 B 站高热分区评论 & 弹幕
- 每周输出 Top 20 疑似新梗候选
- LLM 自动分析每个候选词，输出结构化 MemeRecord
- 通过 REST API 可查询梗库

### MVP 明确不做
- 多平台采集（微博/抖音/NGA）
- 前端 UI
- 用户账户体系
- 付费/商业化功能
- 视觉理解（图片/视频帧分析）

---

## 二、系统架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                     MemeDetector MVP                        │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │ Scheduler│───▶│  Scout   │───▶│Researcher│              │
│  │(APSched) │    │(采集+分词)│    │(AI分析)  │              │
│  └──────────┘    └────┬─────┘    └────┬─────┘              │
│                       │               │                     │
│                  ┌────▼─────┐    ┌────▼─────┐              │
│                  │  DuckDB  │    │Meilisearch│             │
│                  │(词频时序) │    │(梗库检索) │              │
│                  └──────────┘    └─────┬────┘              │
│                                        │                    │
│                                  ┌─────▼────┐              │
│                                  │ FastAPI  │              │
│                                  │(查询接口) │              │
│                                  └──────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### 模块职责

| 模块 | 职责 | 触发方式 |
|------|------|---------|
| **Scheduler** | 定时驱动所有任务 | cron |
| **Scout** | 采集 → 分词 → 词频统计 → 写入 DuckDB | 每日 02:00 |
| **Researcher** | 读取候选词 → LLM 分析 → 写入 Meilisearch | 每周一 06:00 |
| **DuckDB** | 持久化词频时序数据（核心资产） | 被动写入 |
| **Meilisearch** | 梗库全文检索 | 被动写入/主动查询 |
| **FastAPI** | 对外提供 REST 查询接口 | 常驻服务 |

---

## 三、技术栈

| 层次 | 选型 | 版本要求 | 说明 |
|------|------|---------|------|
| 语言 | Python | 3.12+ | asyncio 原生支持 |
| 采集 | bilibili-api (Nemo2011 fork) | latest | 内置 WBI 签名 + curl_cffi 指纹 |
| 调度 | APScheduler | 3.x | 轻量，支持 asyncio |
| 分词 | Jieba | latest | 自定义词典支持 |
| AI 框架 | PydanticAI | latest | 结构化输出与工作流编排 |
| LLM | DeepSeek-V3 | API | 批量筛选；高性价比 |
| 词频存储 | DuckDB | latest | 嵌入式，SQL 支持窗口函数 |
| 梗库检索 | Meilisearch | v1.x | Rust 编写，开箱即用 |
| API 服务 | FastAPI + Uvicorn | latest | 异步，自动 OpenAPI 文档 |
| 容器化 | Docker Compose | v2 | 一键启动 |

---

## 四、目录结构

```
MemeDetector/
├── docker-compose.yml
├── pyproject.toml              # uv 管理依赖
├── .env.example
│
├── meme_detector/
│   ├── __init__.py
│   ├── config.py               # 所有配置（从环境变量读取）
│   │
│   ├── scout/                  # 模块一：采集与发现
│   │   ├── __init__.py
│   │   ├── collector.py        # B站采集（bilibili-api）
│   │   ├── segmenter.py        # Jieba 分词 + 词频统计
│   │   └── scorer.py           # 环比 Score 计算 + 候选词生成
│   │
│   ├── researcher/             # 模块二：AI 分析
│   │   ├── __init__.py
│   │   ├── agent.py            # PydanticAI Agent 定义
│   │   ├── tools.py            # 工作流辅助：火山联网搜索、URL验证
│   │   ├── validator.py        # 来源 URL 真实性验证
│   │   └── models.py           # MemeRecord Pydantic 模型
│   │
│   ├── archivist/              # 模块三：存储
│   │   ├── __init__.py
│   │   ├── duckdb_store.py     # 词频时序写入/查询
│   │   └── meili_store.py      # Meilisearch 写入/查询
│   │
│   ├── api/                    # 模块四：对外接口
│   │   ├── __init__.py
│   │   └── routes.py           # FastAPI 路由
│   │
│   └── scheduler.py            # APScheduler 任务定义
│
├── data/
│   ├── dicts/
│   │   └── userdict.txt        # Jieba 自定义词典（梗词库）
│   └── duckdb/
│       └── freq.db             # 词频数据库（gitignore）
│
└── tests/
    ├── test_scorer.py
    ├── test_agent.py
    └── test_api.py
```

---

## 五、核心模块详细设计

### 5.1 Scout — 采集与发现

#### 采集策略

```python
# 目标分区（MVP 阶段，覆盖高梗密度区）
TARGET_PARTITIONS = [
    "动画",      # tid=1
    "游戏",      # tid=4
    "鬼畜",      # tid=119
    "生活",      # tid=160
]

# 每分区每日：Top 20 热门视频 × 前 500 条热评
# 估算：4 × 20 × 500 = 40,000 条/天，分词后约 200,000 个词元
```

#### 反爬配置

```python
# config.py
BILIBILI_COOKIES: str           # 从环境变量注入，降低风控概率
REQUEST_DELAY_MIN: float = 0.8  # 请求间隔（秒）
REQUEST_DELAY_MAX: float = 2.5
PROXY_URL: str | None = None    # 可选代理
```

#### 词频统计与 Baseline 计算

**DuckDB Schema：**

```sql
-- 每天的词频快照
CREATE TABLE word_freq (
    word        TEXT NOT NULL,
    date        DATE NOT NULL,
    partition   TEXT NOT NULL,
    freq        INTEGER NOT NULL,
    doc_count   INTEGER NOT NULL,   -- 出现在几个视频评论中
    PRIMARY KEY (word, date, partition)
);

-- 候选词记录（待 AI 审核）
CREATE TABLE candidates (
    word        TEXT PRIMARY KEY,
    score       FLOAT NOT NULL,     -- Current / Baseline
    is_new_word BOOLEAN NOT NULL,   -- 首次出现的新词
    detected_at TIMESTAMP DEFAULT NOW(),
    status      TEXT DEFAULT 'pending'  -- pending/accepted/rejected
);
```

**评分公式：**

```python
def compute_score(current_freq: int, baseline_avg: float) -> float:
    """
    baseline_avg: 过去 14 天的日均频率（不含当前窗口）
    新词（baseline=0）单独处理，使用 doc_count 阈值过滤
    """
    if baseline_avg < 0.5:          # 视为新词
        return float('inf')         # 标记为 is_new_word=True
    return current_freq / baseline_avg

# 入选候选名单条件：
# 1. score > 5.0（老词频率 5 倍增长）
# 2. is_new_word=True AND doc_count >= 3（新词至少在 3 个视频出现）
# 3. 过滤停用词、单字词、纯数字
```

---

### 5.2 Researcher — AI 分析

#### Agent 设计（PydanticAI）

```python
# models.py
class MemeRecord(BaseModel):
    id: str                         # 词本身，作为主键
    title: str
    alias: list[str] = []
    definition: str
    origin: str
    category: list[str]             # ["抽象", "谐音", "游戏", ...]
    platform: str = "Bilibili"
    heat_index: int                 # 0-100，由 score 归一化
    lifecycle_stage: str            # emerging / peak / declining
    first_detected_at: date
    source_urls: list[str] = []     # 溯源链接
    confidence_score: float         # 0.0-1.0
    human_verified: bool = False
    updated_at: date
```

#### 三步分析流程

```
Step 1: 快速批量筛选（DeepSeek-V3，batch 模式）
  输入：候选词 + 采样的 10 条上下文评论
  输出：{is_meme: bool, confidence: float, candidate_category: str}
  成本控制：每次最多 50 个候选词一批

Step 2: 深度分析（仅 confidence >= 0.65）
  工作流固定搜索步骤：
    - volcengine_web_search_summary(word) → 优先获取外部背景摘要
    - 摘要不足时，再补 volcengine_web_search(word) 获取网页结果
  LLM 只消费系统已执行好的搜索结果，不直接调用搜索后端
  输出：完整 MemeRecord（含 origin, definition, source_urls）

Step 3: 来源验证
  对 source_urls 中每个 URL 做 HTTP HEAD 请求
  过滤 404/403，保留真实存在的来源
  confidence_score 根据有效来源数量调整
```

#### Prompt 设计原则

- System Prompt 明确角色：「你是互联网亚文化研究员，专注于识别中文网络梗」
- 提供 5 个 few-shot 示例（含正例和反例）
- 要求输出 JSON，字段与 MemeRecord 严格对应
- 反例示例：普通流行语（"内卷"）、活动关键词（某UP名字）、停用词

---

### 5.3 Archivist — 存储层

#### Meilisearch 配置

```json
{
  "searchableAttributes": ["title", "alias", "definition", "origin"],
  "filterableAttributes": ["category", "platform", "lifecycle_stage", "human_verified"],
  "sortableAttributes": ["heat_index", "updated_at", "first_detected_at"],
  "rankingRules": [
    "words", "typo", "proximity", "attribute", "sort", "exactness"
  ]
}
```

#### 数据流向总结

```
DuckDB                    Meilisearch
  │                           │
  ├── word_freq（原始时序）     ├── memes（可检索梗库）
  └── candidates（待审队列）    └── （只存 AI 确认的 MemeRecord）

原则：DuckDB 是 Source of Truth，Meilisearch 是查询加速层
```

---

### 5.4 API — 对外接口

**基础端点（MVP）：**

```
GET  /memes                    # 列表，支持分页、筛选、排序
GET  /memes/{id}               # 详情
GET  /memes/search?q={query}   # 全文检索（代理 Meilisearch）
GET  /candidates               # 待人工审核的候选词列表
POST /candidates/{word}/verify # 人工确认/拒绝（内部使用）
GET  /stats                    # 统计概览（总量、本周新增等）
```

---

## 六、数据流时序图

```
每日 02:00
    │
    ▼
[Scout] 采集 B 站 Top 热门评论 & 弹幕
    │
    ▼
[Scout] Jieba 分词 → 统计词频 → 写入 DuckDB (word_freq)
    │
    ▼（每周一 06:00）
[Scout] 查询 DuckDB，计算环比 Score → 生成 candidates 表

    │
    ▼
[Researcher] Step1: DeepSeek 批量筛选 candidates
    │
    ├── confidence < 0.65 → 标记 rejected，结束
    │
    └── confidence >= 0.65
           │
           ▼
        [Researcher] Step2: 工作流搜索后深度分析
           │
           ▼
        [Researcher] Step3: URL 验证
           │
           ▼
        [Archivist] 写入 Meilisearch
           │
           ▼
        [可选] 发送周报通知（Telegram/邮件）
```

---

## 七、配置与环境变量

```bash
# .env.example

# B站
BILIBILI_SESSDATA=xxx
BILIBILI_BILI_JCT=xxx
BILIBILI_BUVID3=xxx

# LLM
DEEPSEEK_API_KEY=sk-xxx
DEEPSEEK_BASE_URL=https://api.deepseek.com

# 搜索工具（二选一）
SERPER_API_KEY=xxx          # Google Search via Serper.dev（推荐，有免费额度）
# BING_SEARCH_API_KEY=xxx

# Meilisearch
MEILI_URL=http://meilisearch:7700
MEILI_MASTER_KEY=your-master-key

# 采集参数
SCOUT_TOP_N_VIDEOS=20           # 每分区抓取视频数
SCOUT_COMMENTS_PER_VIDEO=500    # 每视频最大评论数
SCOUT_SCORE_THRESHOLD=5.0       # 候选词最低 Score
SCOUT_NEW_WORD_MIN_DOCS=3       # 新词最少出现视频数

# AI 参数
AI_BATCH_SIZE=50
AI_CONFIDENCE_THRESHOLD=0.65
```

---

## 八、Docker Compose 部署

```yaml
# docker-compose.yml
services:
  meilisearch:
    image: getmeili/meilisearch:v1.7
    ports:
      - "7700:7700"
    volumes:
      - ./data/meili:/meili_data
    environment:
      MEILI_MASTER_KEY: ${MEILI_MASTER_KEY}

  app:
    build: .
    depends_on:
      - meilisearch
    volumes:
      - ./data/duckdb:/app/data/duckdb
      - ./data/dicts:/app/data/dicts
    env_file:
      - .env
    ports:
      - "8000:8000"
    command: python -m meme_detector
```

---

## 九、MVP 交付计划（4 周）

### Week 1：基础设施 + 采集
- [ ] 初始化项目结构，配置 Docker Compose
- [ ] 实现 `collector.py`：B站分区 Top 视频评论 & 弹幕采集
- [ ] 实现 `segmenter.py`：Jieba 分词，加载自定义词典
- [ ] 实现 `duckdb_store.py`：词频时序写入
- [ ] 手动跑通 1 个分区的完整采集链路

### Week 2：发现算法
- [ ] 实现 `scorer.py`：环比 Score 计算，生成 candidates 表
- [ ] 补充停用词、单字词过滤逻辑
- [ ] 从小鸡词典手动整理初始词典（~200 词）
- [ ] 积累 7 天词频数据，验证 Score 效果

### Week 3：AI 分析链路
- [ ] 实现 `models.py`：MemeRecord Pydantic 模型
- [ ] 实现 `agent.py`：PydanticAI Agent，三步分析流程
- [ ] 实现 `tools.py`：火山联网搜索与 URL 验证辅助函数
- [ ] 实现 `validator.py`：URL 真实性验证
- [ ] 端到端测试：输入候选词 → 输出 MemeRecord

### Week 4：存储 + API + 收尾
- [ ] 实现 `meili_store.py`：Meilisearch 写入配置
- [ ] 实现 FastAPI 路由（查询 + 人工审核端点）
- [ ] 实现 `scheduler.py`：APScheduler 定时任务
- [ ] 集成测试：全链路跑通
- [ ] 编写 README 和 API 文档

---

## 十、关键风险与预案

| 风险 | 概率 | 影响 | 预案 |
|------|------|------|------|
| B站 API 封禁/风控 | 中 | 高 | Cookie 轮换 + 请求随机延迟 + 可选代理；降级到公开 RSS 接口 |
| DeepSeek API 不稳定 | 低 | 中 | 配置重试（指数退避）；预留 OpenAI 兼容接口切换 |
| LLM 大量幻觉 | 中 | 中 | URL 验证 + confidence 阈值 + 人工审核端点兜底 |
| 词频 Baseline 冷启动 | 确定 | 中 | 前两周仅积累数据不报告；Score 公式对新词单独处理 |
| Meilisearch 数据丢失 | 低 | 低 | 数据来源于 DuckDB，可随时重建索引 |

---

## 十一、后续迭代方向（Post-MVP）

1. **多平台数据源**：加入微博/NGA 做交叉验证，提升置信度
2. **Embedding 语义检测**：用 `bge-m3` 替代纯词频，对谐音梗/变体梗更友好
3. **梗生命周期预测**：基于时序数据训练 Prophet 模型，预测梗的热度走势
4. **推送产品**：每周 Top 10 新梗 Telegram Bot / 邮件报告
5. **多模态**：Gemini 分析表情包图片，识别图片梗
6. **梗关系图谱**：标记"父梗 → 子梗"衍生关系
