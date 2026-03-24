# meme_detector 包

顶层包，包含系统所有模块及命令行入口。

## 模块结构

```
meme_detector/
├── __main__.py       # CLI 入口：serve / scout / research
├── config.py         # 全局配置（从 .env 读取）
├── scheduler.py      # APScheduler 定时任务（每日采集 / 每周分析）
│
├── scout/            # 采集与发现模块
├── researcher/       # AI 分析模块
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
[scout] 每日采集 B站评论/弹幕
    └─▶ Jieba 分词 + 词频统计
        └─▶ DuckDB 写入 word_freq 表（时序数据）
            └─▶ 计算环比 Score → 生成 candidates 表

[researcher] 读取 candidates（status=pending）
    └─▶ Step1: DeepSeek 批量快筛
        └─▶ Step2: 工具调用深度分析（仅高置信度）
            └─▶ Step3: URL 真实性验证
                └─▶ Meilisearch 写入 MemeRecord
```

## 配置

所有配置通过环境变量注入，参见根目录 `.env.example`。
`config.py` 中的 `settings` 对象为全局单例，各模块直接 `from meme_detector.config import settings` 使用。
