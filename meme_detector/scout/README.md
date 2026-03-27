# scout — 采集与发现模块

负责从 B 站抓取数据，并通过 LLM 直接从高赞评论中识别"疑似新梗"候选词。

## 文件

| 文件 | 职责 |
|------|------|
| `collector.py` | 调用 bilibili-api，采集分区 Top 视频的高赞评论（每视频 top 20） |
| `llm_analyzer.py` | 调用 DeepSeek，从评论批次中提取正在传播的梗短语 |
| `scorer.py` | Scout 主流程，串联采集 → LLM 识别 → 写库 |

## 触发方式

- **自动**：每日 02:05 由 `scheduler.py` 调用 `run_scout()`
- **手动**：`python -m meme_detector scout`

## 采集范围（MVP）

| 分区 | tid |
|------|-----|
| 动画 | 1   |
| 游戏 | 4   |
| 鬼畜 | 119 |
| 生活 | 160 |

每分区取排行榜 Top `SCOUT_TOP_N_VIDEOS`（默认 20）个视频，
每视频取前 `SCOUT_COMMENTS_PER_VIDEO`（默认 20）条高赞评论。

## 识别逻辑

```
采集高赞评论
    ↓
分批（每批 100 条）发送给 DeepSeek
    ↓
LLM 识别正在传播的梗短语（多条评论出现相似表达 → 候选）
    ↓
写入 candidates 表，等待 Researcher 深度分析
```

LLM 判断依据：
- 多条评论出现相似的表达方式（即使措辞不同）
- 带有特定网络含义，不是字面意思
- 感觉是在引用/模仿某个说法，或是句式模板

## 反爬策略

- 每次请求前随机 sleep `[SCOUT_DELAY_MIN, SCOUT_DELAY_MAX]` 秒（默认 0.8~2.5s）
- 支持通过 `SCOUT_PROXY_URL` 配置 HTTP 代理
- 配置 `BILIBILI_SESSDATA` 等 Cookie 可降低风控概率
- 评论接口遇到 `412/429/5xx` 会按 `SCOUT_COMMENT_RETRY_TIMES` 做有限重试

## 扩展：增加新分区

在 `collector.py` 的 `TARGET_PARTITIONS` 列表中追加 `(分区名, RankType.XXX)` 即可。
