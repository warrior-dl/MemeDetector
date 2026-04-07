# MemeDetector

自动发现并归档 B 站亚文化新梗的 AI Pipeline。

每天采集高热分区视频元信息与评论，先把原始快照写入 DuckDB，再由 `Miner` 结合标题、简介、标签、视频内容和评论，对评论做“潜在梗 / 圈内知识”初筛，最后由 `Researcher` 做候选提取、联网搜索、溯源分析并入库到可全文检索的梗百科。
`Scout` 只做评论和视频元数据采集，不调用 LLM；`Miner` 负责显式调用 BibiGPT 解析视频内容并给评论打分；`Researcher` 只消费 Miner 高价值线索做进一步搜索和解析。超过 15 分钟的视频会自动跳过并缓存结果。评论中的图片会额外下载到本地资产目录，并在 DuckDB 中保存元数据和关联关系，便于后续多模态分析与重跑。

## 快速开始

### 本地开发（推荐）

基础设施跑在 Docker，app 在宿主机直接运行，方便断点调试。

```bash
# 1. 复制配置，填入 B站 Cookie 和 DeepSeek API Key
cp .env.example .env
# 确认 .env 中 MEILI_URL=http://localhost:7700

# 2. 安装依赖
pip install -e ".[dev]"

# 3. 启动 Meilisearch
docker compose up -d

# 4. 手动触发一次采集（写入原始视频/评论快照）
python -m meme_detector scout

# 5. 手动触发一次 Miner 评论线索挖掘
python -m meme_detector miner

# 6. 手动触发一次 AI 分析
python -m meme_detector research

# 7. 启动 API 服务（含定时调度器）
python -m meme_detector serve
# 访问 http://localhost:8000/docs 查看接口文档
# 访问 http://localhost:8000/admin 查看前端管理台
# 访问 http://localhost:8000/admin/scout 查看 Scout 原始快照调试页
# 访问 http://localhost:8000/admin/miner 查看 Miner 评论线索调试页
# 访问 http://localhost:8000/admin/candidates 查看候选梗分页管理页
# 访问 http://localhost:8000/admin/conversations 查看 Agent 对话记录页
```

### 生产部署（全容器化）

```bash
# .env 中将 MEILI_URL 改为 http://meilisearch:7700
docker compose --profile prod up -d
```

### 两种模式的区别

| | 本地开发 | 生产部署 |
|---|---|---|
| Meilisearch | Docker | Docker |
| app | 宿主机直接运行 | Docker (`prod` profile) |
| `MEILI_URL` | `http://localhost:7700` | `http://meilisearch:7700` |
| 调试 | 支持断点 / 热重载 | 不支持 |

## 架构

```
B站视频元信息/评论
    │
    ▼
[scout]  每日采集 → DuckDB 原始视频 / 评论 / 图片快照
    │
    ▼
[miner]  评论初筛 → BibiGPT 视频内容解析 → 评论线索评分
    │
    ▼
[researcher]  候选词提取 → DeepSeek 批量筛选 → 联网搜索 → 深度溯源 → URL验证
    │
    ▼
[Meilisearch]  梗库全文检索
    │
    ▼
[FastAPI]  REST API / Admin
```

## 目录

```
meme_detector/       # 源码包（各子目录含 README.md）
├── scout/           # 采集 + 原始快照入库
├── miner/           # 视频内容解析 + 评论线索初筛
├── researcher/      # 候选提取 + AI 三步分析（快筛→深度→验证）
├── archivist/       # DuckDB 原始/候选存储 + Meilisearch 梗库
└── api/             # FastAPI REST 接口

data/
└── duckdb/freq.db       # DuckDB 数据库（原始快照 / 候选 / 缓存）

docs/                # 设计文档
tests/               # 单元测试
```

## 技术栈

| 用途 | 选型 |
|------|------|
| 数据采集 | bilibili-api (Nemo2011 fork) |
| AI 框架 | PydanticAI + DeepSeek-V3 |
| 原始/候选存储 | DuckDB |
| 梗库检索 | Meilisearch |
| API 服务 | FastAPI + Uvicorn |
| 定时调度 | APScheduler |
| 容器化 | Docker Compose |

## 测试

```bash
pip install -e ".[dev]"
pytest tests/ -v
```
