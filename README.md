# MemeDetector

自动发现并归档 B 站亚文化新梗的 AI Pipeline。

每天采集高热分区评论与弹幕，通过词频环比算法发现疑似新梗，由 DeepSeek-V3 完成溯源分析，最终入库到可全文检索的梗百科。

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

# 4. 手动触发一次采集（冷启动积累基线数据）
python -m meme_detector scout

# 5. 手动触发一次 AI 分析
python -m meme_detector research

# 6. 启动 API 服务（含定时调度器）
python -m meme_detector serve
# 访问 http://localhost:8000/docs 查看接口文档
# 访问 http://localhost:8000/admin 查看前端管理台
# 访问 http://localhost:8000/admin/candidates 查看候选梗分页管理页
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
B站评论/弹幕
    │
    ▼
[scout]  每日采集 → Jieba 分词 → DuckDB 词频时序
    │
    ▼  (每周一)
[researcher]  DeepSeek 批量筛选 → 深度溯源 → URL验证
    │
    ▼
[Meilisearch]  梗库全文检索
    │
    ▼
[FastAPI]  REST API
```

## 目录

```
meme_detector/       # 源码包（各子目录含 README.md）
├── scout/           # 采集 + 词频统计 + 候选词发现
├── researcher/      # AI 三步分析（快筛→深度→验证）
├── archivist/       # DuckDB 时序存储 + Meilisearch 梗库
└── api/             # FastAPI REST 接口

data/
├── dicts/userdict.txt   # Jieba 自定义梗词典
└── duckdb/freq.db       # 词频时序数据库（核心数据资产）

docs/                # 设计文档
tests/               # 单元测试
```

## 技术栈

| 用途 | 选型 |
|------|------|
| 数据采集 | bilibili-api (Nemo2011 fork) |
| 中文分词 | Jieba |
| AI 框架 | PydanticAI + DeepSeek-V3 |
| 词频存储 | DuckDB |
| 梗库检索 | Meilisearch |
| API 服务 | FastAPI + Uvicorn |
| 定时调度 | APScheduler |
| 容器化 | Docker Compose |

## 测试

```bash
pip install -e ".[dev]"
pytest tests/ -v
```
