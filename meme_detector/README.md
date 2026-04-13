# meme_detector 包

顶层包，包含系统所有模块及命令行入口。

## 模块结构

```
meme_detector/
├── __main__.py       # CLI 入口：serve / scout / miner_insights / miner_bundles / miner / research
├── config.py         # 全局配置（从 .env 读取）
├── scheduler.py      # APScheduler 定时任务（每日采集 / 每周分析）
├── run_tracker.py    # Pipeline 运行记录上下文与落盘封装
│
├── scout/            # 采集与原始入库模块
├── miner/            # 评论线索初筛 + 证据包生成
├── researcher/       # 评论证据包裁决模块
├── archivist/        # 存储层
└── api/              # REST API
```

## 命令

```bash
# 启动 API 服务 + 调度器（生产模式）
python -m meme_detector serve

# 手动触发一次采集（调试 / 冷启动积累数据）
python -m meme_detector scout

# 手动触发 Miner Stage 1：评论线索初筛
python -m meme_detector miner_insights

# 手动触发 Miner Stage 2：证据包生成
python -m meme_detector miner_bundles

# 串行执行两个 Miner 阶段
python -m meme_detector miner

# 手动触发一次 AI 分析（调试 / 补跑）
python -m meme_detector research
```

## 数据流

```
[scout] 每日采集 B站视频元信息/评论/评论图片
    └─▶ DuckDB 写入 scout_raw_videos / scout_raw_comments / media_assets

[miner stage 1] 先处理未消费的 scout_raw_videos
    ├─▶ 显式调用 BibiGPT 获取视频背景
    │   └─▶ DuckDB video_context_cache 缓存视频总结 / 字幕摘要
    └─▶ 把 标题 + 简介 + 标签 + 视频内容 + 评论 交给 LLM
        └─▶ 产出 miner_comment_insights 评论线索表

[miner stage 2] 消费高价值 miner_comment_insights
    └─▶ 生成 comment_insights / spans / hypotheses / evidences

[researcher] 优先消费 queued comment bundles
    └─▶ 基于 spans / hypotheses / evidences 做最终裁决
        ├─▶ 输出 ResearchDecision
        ├─▶ 仅在 accept / rewrite_title 时生成 MemeRecord
        ├─▶ URL 真实性验证
        └─▶ Meilisearch + DuckDB 写入正式词条

[serve / 手动命令]
    └─▶ run_tracker 记录 pipeline_runs
        └─▶ 工作台 / /bundles /library /pipeline 可视化查看
```

## 配置

所有配置通过环境变量注入，参见根目录 `.env.example`。
`config.py` 中的 `settings` 对象为全局单例，各模块直接 `from meme_detector.config import settings` 使用。

LLM 配置采用两级覆盖：

- `LLM_*`：默认模型，供未单独指定的模块继承
- `MINER_LLM_*` / `RESEARCH_LLM_*`：按 pipeline 覆盖

## 运行审计与管理

- `pipeline_runs`：记录 Scout / Miner Stage 1 / Miner Stage 2 / Researcher 每次运行状态、摘要和结果数量
- `agent_conversations`：记录 Agent 对话与运行摘要
- `video_context_cache`：缓存视频背景分析结果，避免重复请求外部 API
- `miner_comment_insights`：保存 Miner Stage 1 的评论线索与 Stage 2 去向状态
- `comment_insights` / `comment_spans` / `hypotheses` / `evidences` / `research_decisions`：评论证据包与裁决结果
- `scout_raw_videos`：保存 Scout 采集到的原始视频/评论快照，供 Miner 挖掘
- `scout_raw_comments` / `media_assets` / `comment_media_links`：保存结构化评论、评论图片资产及评论到图片的关联关系
- 管理台入口：
  - `/`
  - `/bundles`
  - `/library`
  - `/pipeline`
