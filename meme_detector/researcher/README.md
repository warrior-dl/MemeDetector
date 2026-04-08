# researcher — AI 分析模块

优先从 Miner 产出的高价值评论线索中提取候选词，再执行 AI 分析，生成结构化的 `MemeRecord` 并写入梗库。

## 文件

| 文件 | 职责 |
|------|------|
| `models.py` | `MemeRecord`、`QuickScreenResult`、`ResearchRunResult` 等结果模型 |
| `tools.py` | 工具函数：火山引擎联网搜索、URL 真实性验证 |
| `bootstrap.py` | Step 0：从 Miner 高价值线索中提取候选词并落库 |
| `screening.py` | Step 1：批量快筛与通过/拒绝/待重试分流 |
| `deep_analysis.py` | Step 2/3：深度分析、搜索上下文准备、Agent 对话落库 |
| `persistence.py` | Research 阶段候选读取、状态更新、入库封装 |
| `agent.py` | Research 编排层，对外稳定入口 `run_research()` |

## 触发方式

- **自动**：每周一 06:00 由 `scheduler.py` 调用 `run_research()`
- **手动**：`python -m meme_detector research`

当前流程不会自动触发 `miner`。
如果还存在 `scout_raw_videos.miner_status='pending'` 的视频，`research` 会直接退出并提示先手动运行 `miner`。
如果 `serve` 正在运行，推荐直接在根路径工作台 `/` 中触发任务，避免 DuckDB 锁冲突。

## 四步分析流程

```
Step 0  候选词提取
        前置条件：不存在待 Miner 处理的视频
        输入：全部未处理的 Miner 评论线索（高价值优先）
        模型：Research LLM（JSON 模式，默认继承 `LLM_MODEL`）
        输出：候选词队列 { word, confidence, reason, related_bvids, sample_comments }
        落库：写入 candidates 表，并将对应 Miner 线索标记 processed

Step 1  批量快筛
        输入：全部 pending 候选词（最多 AI_BATCH_SIZE=50 个/批）
        模型：Research LLM（JSON 模式，默认继承 `LLM_MODEL`）
        输出：QuickScreenResult { is_meme, confidence, reason }
        分流：
          is_meme=true 且 confidence ≥ AI_CONFIDENCE_THRESHOLD → 进入深度分析
          明确低分/非梗 → 标记 rejected
          模型未返回该词 → 保留 pending，记为待重试
        诊断：
          兼容 results/items/data 等多种 JSON 结构
          若整批响应无法解析，会直接报错并打印原始响应摘要，避免静默结束

Step 2  深度分析（仅 Step1 通过的词）
        模型：OpenAI-compatible 模型，经 PydanticAI + provider 工厂统一接入
        超时：RESEARCH_LLM_TIMEOUT_SECONDS，避免单词条无限阻塞
        主流程预取：
          Miner 关联视频背景优先复用缓存，必要时补拉视频上下文（带 DuckDB 缓存，超 15 分钟跳过）
        Agent 可调用工具：
          volcengine_web_search_summary("[word] 梗 来源") → 先拿火山引擎总结版搜索结果
          volcengine_web_search("[word] 梗 来源")        → 总结不够时再补火山引擎普通网页结果
        输出：完整 MemeRecord（含 definition、origin、source_urls 等）

Step 3  来源验证
        对 source_urls 列表发送 HTTP HEAD 请求
        过滤 4xx/5xx 或超时的死链
        有效来源数量不足时按比例下调 confidence_score
        → 等待写入 Meilisearch 成功
        → 同步写入 DuckDB `meme_records` 镜像表
```

`run_research()` 返回：

```python
ResearchRunResult(
    pending_count=...,
    bootstrapped_count=...,
    screened_count=...,
    deep_analysis_count=...,
    accepted_count=...,
    rejected_count=...,
    accepted_records=[...],
    rejected_words=[...],
    screen_failed_words=[...],
    failed_words=[...],
    blocked_pending_video_count=...,
)
```

## 依赖配置

| 环境变量 | 用途 | 必填 |
|----------|------|------|
| `LLM_API_KEY` | 默认 OpenAI-compatible API 密钥 | ✅ |
| `LLM_BASE_URL` | 默认接口地址 | ✅ |
| `LLM_MODEL` | 默认模型名 | ✅ |
| `LLM_PROVIDER` | `auto/openai/deepseek/moonshotai` | 可选 |
| `RESEARCH_LLM_API_KEY` | Research 专属密钥 | 可选 |
| `RESEARCH_LLM_BASE_URL` | Research 专属接口地址 | 可选 |
| `RESEARCH_LLM_MODEL` | Research 专属模型名 | 可选 |
| `RESEARCH_LLM_PROVIDER` | Research 专属 provider 提示 | 可选 |
| `WEB_SEARCH_API_KEY` | 火山引擎联网搜索 API Key | 可选（缺失则跳过 Web 搜索） |
| `BIBIGPT_API_TOKEN` | BibiGPT 视频总结 API | 可选（通常由 Miner 消费，缺失则仅保留基础元数据） |

## 幻觉防护

1. **置信度阈值**：低于 0.65 直接拒绝，不进入深度分析
2. **URL 验证**：HTTP HEAD 检查来源真实性，过滤 AI 编造的链接
3. **人工兜底**：所有入库记录 `human_verified=false`，可通过 API 端点复核
4. **重试机制**：`tenacity` 指数退避重试（最多 3 次），防止 API 偶发失败

## 扩展：接入其他 LLM

统一入口在 `meme_detector.llm_factory`：

- 默认读取 `LLM_*`
- `Researcher` 可通过 `RESEARCH_LLM_*` 单独覆盖
- `LLM_PROVIDER/RESEARCH_LLM_PROVIDER` 支持 `auto/openai/deepseek/moonshotai`

如果目标服务是标准 OpenAI-compatible 接口，通常只需要改 `*_LLM_BASE_URL` 和 `*_LLM_MODEL`。
只有遇到特定 provider 的工具 / schema 兼容性差异时，才需要显式指定 `*_LLM_PROVIDER`。
