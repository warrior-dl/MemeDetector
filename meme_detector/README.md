# meme_detector 包

顶层包，包含系统所有模块及命令行入口。

## 模块结构

```
meme_detector/
├── __main__.py       # CLI 入口：serve / scout / miner / research
├── config.py         # 全局配置（从 .env 读取）
├── scheduler.py      # APScheduler 定时任务（每日采集 / 每周分析）
├── run_tracker.py    # Pipeline 运行记录上下文与落盘封装
│
├── scout/            # 采集与原始入库模块
├── miner/            # 视频内容解析 + 评论线索初筛
├── researcher/       # 候选提取 + AI 分析模块
├── archivist/        # 存储层
└── api/              # REST API
```

## 命令

```bash
# 启动 API 服务 + 调度器（生产模式）
python -m meme_detector serve

# 手动触发一次采集（调试 / 冷启动积累数据）
python -m meme_detector scout

# 手动触发一次 Miner 评论线索挖掘（调试 / 补跑）
python -m meme_detector miner

# 手动触发一次 AI 分析（调试 / 补跑）
python -m meme_detector research
```

## 数据流

```
[scout] 每日采集 B站视频元信息/评论/评论图片
    └─▶ DuckDB 写入 scout_raw_videos / scout_raw_comments / media_assets

[miner] 先处理未消费的 scout_raw_videos
    ├─▶ 显式调用 BibiGPT 获取视频背景
    │   └─▶ DuckDB video_context_cache 缓存视频总结 / 字幕摘要
    └─▶ 把 标题 + 简介 + 标签 + 视频内容 + 评论 交给 LLM
        └─▶ 产出 miner_comment_insights 评论线索表

[researcher] 优先消费未处理的 miner_comment_insights
    └─▶ Step0: 从高价值评论线索提取候选词 → 写入 candidates 表
        └─▶ 读取 candidates（status=pending）
    └─▶ Step1: DeepSeek 批量快筛
        └─▶ Step2: 主流程预取视频背景 + Agent 深度分析（仅高置信度）
            ├─▶ volcengine_web_search_summary 先拿火山引擎总结版外部搜索上下文
            ├─▶ volcengine_web_search 仅在总结不足时补火山引擎普通网页结果
            ├─▶ Agent 对话上下文写入 DuckDB agent_conversations
            └─▶ Step3: URL 真实性验证
                └─▶ Meilisearch 写入 MemeRecord

[serve / 手动命令]
    └─▶ run_tracker 记录 pipeline_runs
        └─▶ 工作台 / /candidates /library /pipeline 可视化查看
```

## 配置

所有配置通过环境变量注入，参见根目录 `.env.example`。
`config.py` 中的 `settings` 对象为全局单例，各模块直接 `from meme_detector.config import settings` 使用。

## 运行审计与管理

- `pipeline_runs`：记录 Scout / Miner / Researcher 每次运行状态、摘要和结果数量
- `agent_conversations`：记录 Researcher 对单个候选词的完整 Agent 对话
- `video_context_cache`：缓存视频背景分析结果，避免重复请求外部 API
- `miner_comment_insights`：保存 Miner 对评论的初步判定结果，供 Researcher 消费
- `scout_raw_videos`：保存 Scout 采集到的原始视频/评论快照，供 Miner 挖掘
- `scout_raw_comments` / `media_assets` / `comment_media_links`：保存结构化评论、评论图片资产及评论到图片的关联关系
- 管理台入口：
  - `/`
  - `/candidates`
  - `/library`
  - `/pipeline`
