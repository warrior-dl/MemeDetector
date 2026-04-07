# scout — 采集与入库模块

负责从 B 站抓取数据，并将视频元信息和评论原始快照直接写入 DuckDB。

## 文件

| 文件 | 职责 |
|------|------|
| `collector.py` | 调用 bilibili-api，采集分区 Top 视频的高赞评论（每视频 top 20） |
| `llm_analyzer.py` | 旧版实验模块，当前 `scout` 流程不再调用 |
| `models.py` | `ScoutRunResult` 运行结果模型 |
| `persistence.py` | Scout 原始快照入库封装 |
| `scorer.py` | Scout 编排层，串联采集 → flatten → 原始快照写库 |

## 触发方式

- **自动**：每日 02:05 由 `scheduler.py` 调用 `run_scout()`
- **手动**：`python -m meme_detector scout`

如果 `serve` 正在运行，手动调试更推荐走 `/admin` 页面内的任务触发按钮，避免额外起新进程与 DuckDB 写锁冲突。

## 采集范围（MVP）

| 分区 | tid |
|------|-----|
| 动画 | 1   |
| 游戏 | 4   |
| 鬼畜 | 119 |
| 生活 | 160 |

每分区取排行榜 Top `SCOUT_TOP_N_VIDEOS`（默认 20）个视频，
每视频取前 `SCOUT_COMMENTS_PER_VIDEO`（默认 20）条高赞评论。

## 入库逻辑

```
采集高赞评论和视频元信息
    ↓
整理为扁平化原始快照结构
    ↓
写入 scout_raw_videos 原始快照表
    ↓
等待 Miner 消费评论并补充线索
    ↓
等待 Researcher 提取候选词并深度分析
```

当前 `Scout` 不调用 LLM，只负责采集和原始数据入库。
候选词判断、深度理解、溯源和最终判定都留给后续 `Miner` / `Researcher` 阶段完成。

`run_scout()` 返回：

```python
ScoutRunResult(
    target_date="YYYY-MM-DD",
    video_count=...,
    comment_count=...,
)
```

## 反爬策略

- 每次请求前随机 sleep `[SCOUT_DELAY_MIN, SCOUT_DELAY_MAX]` 秒（默认 0.8~2.5s）
- 支持通过 `SCOUT_PROXY_URL` 配置 HTTP 代理
- 配置 `BILIBILI_SESSDATA` 等 Cookie 可降低风控概率
- 评论接口遇到 `412/429/5xx` 会按 `SCOUT_COMMENT_RETRY_TIMES` 做有限重试

## 扩展：增加新分区

在 `collector.py` 的 `TARGET_PARTITIONS` 列表中追加 `(分区名, RankType.XXX)` 即可。
