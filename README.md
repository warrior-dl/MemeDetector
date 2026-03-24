# MemeDetector

自动发现并归档 B 站亚文化新梗的 AI Pipeline。

每天采集高热分区评论与弹幕，通过词频环比算法发现疑似新梗，由 DeepSeek-V3 完成溯源分析，最终入库到可全文检索的梗百科。

## 快速开始

```bash
# 1. 复制配置文件，填入 B站 Cookie 和 DeepSeek API Key
cp .env.example .env

# 2. 启动 Meilisearch
docker compose up meilisearch -d

# 3. 手动触发一次采集（积累冷启动数据）
python -m meme_detector scout

# 4. 手动触发一次 AI 分析
python -m meme_detector research

# 5. 启动 API 服务（含定时调度器）
python -m meme_detector serve
# 访问 http://localhost:8000/docs 查看接口文档
```

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
