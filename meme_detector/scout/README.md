# scout — 采集与发现模块

负责从 B 站抓取数据，并通过词频环比算法识别"疑似新梗"候选词。

## 文件

| 文件 | 职责 |
|------|------|
| `collector.py` | 调用 bilibili-api，采集分区 Top 视频的评论和弹幕 |
| `segmenter.py` | Jieba 分词，输出去噪后的词频统计 |
| `scorer.py`    | Scout 主流程，串联采集→分词→写库→Score 计算 |

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
每视频取前 `SCOUT_COMMENTS_PER_VIDEO`（默认 500）条热评 + 全量弹幕。

## Score 算法

```
老词：Score = current_freq / baseline_avg（过去14天日均频率）
      Score >= SCOUT_SCORE_THRESHOLD(5.0) → 进入候选

新词：baseline 期间从未出现
      doc_count >= SCOUT_NEW_WORD_MIN_DOCS(3) → 进入候选
```

分母保护：`baseline_avg < 0.5` 视为新词，避免除零。

## 反爬策略

- 每次请求前随机 sleep `[SCOUT_DELAY_MIN, SCOUT_DELAY_MAX]` 秒（默认 0.8~2.5s）
- 支持通过 `SCOUT_PROXY_URL` 配置 HTTP 代理
- 配置 `BILIBILI_SESSDATA` 等 Cookie 可降低风控概率

## 扩展：增加新分区

在 `collector.py` 的 `TARGET_PARTITIONS` 列表中追加 `(分区名, tid)` 即可。
