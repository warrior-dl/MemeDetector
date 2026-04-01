# meme_detector 包

顶层包，包含系统所有模块及命令行入口。

## 模块结构

```
meme_detector/
├── __main__.py       # CLI 入口：serve / scout / research
├── config.py         # 全局配置（从 .env 读取）
├── scheduler.py      # APScheduler 定时任务（每日采集 / 每周分析）
├── run_tracker.py    # Pipeline 运行记录上下文与落盘封装
│
├── scout/            # 采集与原始入库模块
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

# 手动触发一次 AI 分析（调试 / 补跑）
python -m meme_detector research
```

## 数据流

```
[scout] 每日采集 B站视频元信息/评论
    └─▶ DuckDB 写入 scout_raw_videos 原始快照表

[researcher] 先处理未消费的 scout_raw_videos
    └─▶ Step0: DeepSeek 提取候选词 → 写入 candidates 表
        └─▶ 读取 candidates（status=pending）
    └─▶ Step1: DeepSeek 批量快筛
        └─▶ Step2: 主流程预取视频背景 + Agent 深度分析（仅高置信度）
            ├─▶ 从 Scout 候选读取评论对应视频
            ├─▶ 主流程显式调用 BibiGPT 获取视频背景
            │   └─▶ DuckDB video_context_cache 缓存视频总结 / 字幕摘要
            ├─▶ bilibili_search 搜索补充相关视频
            ├─▶ web_search 搜索外部背景
            ├─▶ Agent 对话上下文写入 DuckDB agent_conversations
            └─▶ Step3: URL 真实性验证
                └─▶ Meilisearch 写入 MemeRecord

[serve / 手动命令]
    └─▶ run_tracker 记录 pipeline_runs
        └─▶ 管理台 /admin /admin/candidates /admin/conversations 可视化查看
```

## 配置

所有配置通过环境变量注入，参见根目录 `.env.example`。
`config.py` 中的 `settings` 对象为全局单例，各模块直接 `from meme_detector.config import settings` 使用。

## 运行审计与管理

- `pipeline_runs`：记录 Scout / Researcher 每次运行状态、摘要和结果数量
- `agent_conversations`：记录 Researcher 对单个候选词的完整 Agent 对话
- `video_context_cache`：缓存视频背景分析结果，避免重复请求外部 API
- `scout_raw_videos`：保存 Scout 采集到的原始视频/评论快照，供 Researcher 提取候选词
- 管理台入口：
  - `/admin`
  - `/admin/candidates`
  - `/admin/conversations`
