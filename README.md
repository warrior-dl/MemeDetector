# MemeDetector

自动发现并归档 B 站亚文化新梗的 AI Pipeline。

每天采集高热分区视频元信息与评论，先把原始快照写入 DuckDB，再由 `Miner` 结合标题、简介、标签、视频内容和评论，对评论做“潜在梗 / 圈内知识”初筛，最后由 `Researcher` 做候选提取、联网搜索、溯源分析并入库到可全文检索的梗百科。
`Scout` 只做评论和视频元数据采集，不调用 LLM；`Miner` 负责显式调用 BibiGPT 解析视频内容并给评论打分；`Researcher` 只消费 Miner 高价值线索做进一步搜索和解析。超过 15 分钟的视频会自动跳过并缓存结果。评论中的图片会额外下载到本地资产目录，并在 DuckDB 中保存元数据和关联关系，便于后续多模态分析与重跑。

## 当前状态

- 三条 pipeline 现在支持手动独立运行：`scout`、`miner`、`research` 不再要求强绑定串行触发
- `Researcher` 不会自动先跑 `Miner`；如果存在待 `Miner` 处理的视频，会提示你先手动执行
- 新的管理界面入口为 `http://localhost:8000/`
- 旧的 `/admin` 静态页已移除
- 运行日志默认写入 `logs/app.jsonl`，便于按 `run_id`、候选词、异常类型排查

## 快速开始

### 本地开发（推荐）

基础设施跑在 Docker，app 在宿主机直接运行，方便断点调试。

```bash
# 1. 复制配置，填入 B站 Cookie 和默认 LLM 的 OpenAI-compatible API Key
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
# 访问 http://localhost:8000/ 查看新的前端工作台

# 如果 serve 已在运行，优先通过根路径工作台手动触发 Scout / Miner / Research
# 避免另起 CLI 进程写 DuckDB 时产生跨进程文件锁冲突
```

### 手动调试建议

推荐按下面的方式调试，而不是一口气跑完整调度：

```bash
# 1. 先采样新视频/评论
python -m meme_detector scout

# 2. 单独跑评论线索挖掘，确认 Miner 输出是否正常
python -m meme_detector miner

# 3. 单独跑候选分析和入库
python -m meme_detector research
```

适用场景：

- 调试 `Miner` 的评论 JSON 输出、视频上下文、超时和限流问题
- 调试 `Researcher` 的快筛、深度分析、搜索和入库逻辑
- 避免某一步失败后整条 pipeline 从头开始

如果你已经启动了 `python -m meme_detector serve`，更推荐直接通过根路径工作台手动触发任务，而不是并行启动多个 CLI 进程，以减少 DuckDB 文件锁冲突。

### 日志

默认会同时输出：

- 控制台 Rich 日志
- JSONL 文件日志：`logs/app.jsonl`

本地排查推荐：

```bash
tail -f logs/app.jsonl
rg 'research_candidate_failed|pipeline_run_failed|run_id' logs/app.jsonl
```

### LLM 配置

- `LLM_*` 是全局默认模型配置
- `MINER_LLM_*` 和 `RESEARCH_LLM_*` 可分别覆盖各自 pipeline
- 默认通过 `pydantic_ai + openai-python` 接入 OpenAI-compatible 模型
- `*_LLM_PROVIDER` 支持 `auto/openai/deepseek/moonshotai`

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
[researcher]  候选词提取 → LLM 批量快筛 → 联网搜索 → 深度溯源 → URL验证
    │
    ▼
[Meilisearch]  梗库全文检索
    │
    ▼
[FastAPI]  REST API / Workbench
```

## 管理界面

### 新工作台 `/`

基于 `React + TypeScript + Vite + Ant Design + TanStack Query`，作为当前主入口。

- `Dashboard`: 看整体运行情况、最近对话、异常审计
- `候选工作台`: 看候选词、来源线索、关联视频、Researcher 对话，并执行人工审核
- `梗库`: 查看已入库词条，支持详情抽屉和人工校验
- `Pipeline`: 查看调度计划、运行记录、单次运行详情，并手动触发任务

## 目录

```
meme_detector/       # 源码包（各子目录含 README.md）
├── scout/           # 采集 + 原始快照入库
├── miner/           # 视频内容解析 + 评论线索初筛
├── researcher/      # 候选提取 + AI 三步分析（快筛→深度→验证）
├── archivist/       # DuckDB 原始/候选存储 + Meilisearch 梗库
└── api/             # FastAPI REST 接口

frontend/            # 新版前端工作台（Vite）
├── src/app/         # 路由与整体壳层
├── src/features/    # 按 feature 拆分的数据 hooks
├── src/pages/       # Dashboard / Candidates / Library / Pipeline
├── src/ui/          # 通用展示组件
└── dist/            # 构建产物，由 FastAPI 挂载到 /

data/
└── duckdb/freq.db       # DuckDB 数据库（原始快照 / 候选 / 缓存）

docs/                # 设计文档
tests/               # 单元测试
```

## 技术栈

| 用途 | 选型 |
|------|------|
| 数据采集 | bilibili-api (Nemo2011 fork) |
| AI 框架 | PydanticAI + OpenAI-compatible LLM |
| 原始/候选存储 | DuckDB |
| 梗库检索 | Meilisearch |
| API 服务 | FastAPI + Uvicorn |
| 定时调度 | APScheduler |
| 容器化 | Docker Compose |

## 前端开发

```bash
cd frontend
npm install
npm run dev
```

- Vite 开发服务器默认把 `/api` 代理到 `http://127.0.0.1:8000`
- 生产构建使用 `npm run build`
- 构建产物位于 `frontend/dist`
- FastAPI 会把构建产物挂载到 `/`

## 测试

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

前端构建检查：

```bash
cd frontend
npm run build
```
