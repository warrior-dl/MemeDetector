# api — REST API 模块

基于 FastAPI 提供对外查询接口及内部运营工具。

## 文件

| 文件 | 职责 |
|------|------|
| `app.py`    | FastAPI 应用工厂，注册 API 路由、静态资源与管理页 |
| `routes.py` | 所有 REST API 路由处理函数 |

## 管理页

| 页面 | 路径 | 说明 |
|------|------|------|
| 总览 | `/admin` | 查看统计、调度状态、运行记录、最近结果 |
| Scout 原始数据 | `/admin/scout` | 查看 Scout 写入 DuckDB 的原始视频/评论快照 |
| Miner 评论线索 | `/admin/miner` | 查看 Miner 对评论的初筛结果、理由与视频上下文 |
| 候选梗队列 | `/admin/candidates` | 分页查看全部候选梗，支持清空 |
| 候选梗来源线索 | `/admin/candidate-sources?word=` | 查看单个候选梗关联的视频引用与来源评论线索 |
| Agent 对话 | `/admin/conversations` | 查看 Researcher 对每个候选词的完整对话上下文 |

## 接口列表

### 梗库查询

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/memes` | 梗列表，支持分页、过滤、排序 |
| `GET` | `/api/v1/memes/search?q=` | 全文检索（代理 Meilisearch） |
| `GET` | `/api/v1/memes/{id}` | 单条梗详情 |

**过滤参数（`/memes`）：**

| 参数 | 类型 | 示例 |
|------|------|------|
| `category` | string | `抽象` / `谐音` / `游戏` |
| `lifecycle` | string | `emerging` / `peak` / `declining` |
| `verified_only` | bool | `true` |
| `sort_by` | string | `heat_index:desc`（默认） |

### Scout 原始数据（内部）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/scout/raw-videos` | Scout 原始视频快照分页列表 |
| `GET` | `/api/v1/scout/raw-videos/{bvid}?collected_date=` | 单条原始快照详情 |
| `GET` | `/api/v1/miner/comment-insights` | Miner 评论线索分页列表 |
| `GET` | `/api/v1/miner/comment-insights/{insight_id}` | 单条 Miner 评论线索详情 |
| `GET` | `/api/v1/media-assets/{asset_id}` | 图片资产元数据 |
| `GET` | `/api/v1/media-assets/{asset_id}/content` | 图片资产本地文件内容 |

### 候选词管理（内部）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/candidates` | 候选词列表（简表） |
| `GET` | `/api/v1/candidates/page` | 候选词分页列表（完整字段） |
| `GET` | `/api/v1/candidates/{word}/sources` | 单个候选词的来源视频与评论线索 |
| `DELETE` | `/api/v1/candidates` | 删除全部候选词 |
| `POST` | `/api/v1/candidates/{word}/verify?action=accept\|reject` | 人工审核 |
| `POST` | `/api/v1/memes/{id}/verify?verified=true` | 标记梗为人工验证 |

### 运行记录 / 调度 / Agent 对话

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/runs` | Pipeline 运行记录列表 |
| `GET` | `/api/v1/runs/{run_id}` | 单次运行详情 |
| `GET` | `/api/v1/jobs` | APScheduler 任务概览 |
| `POST` | `/api/v1/jobs/{job_name}/run` | 在当前 `serve` 进程内手动触发 Scout / Miner / Research |
| `GET` | `/api/v1/agent-conversations` | Agent 对话分页列表 |
| `GET` | `/api/v1/agent-conversations/{conversation_id}` | 单条 Agent 对话详情 |

### 统计

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/stats` | 候选词统计 + 梗库总量 |

## 调试

启动后访问自动生成的 Swagger 文档：
```
http://localhost:8000/docs
```

管理台入口：

```text
http://localhost:8000/admin
http://localhost:8000/admin/scout
http://localhost:8000/admin/miner
http://localhost:8000/admin/candidates
http://localhost:8000/admin/candidate-sources?word=抽象圣经
http://localhost:8000/admin/conversations
```

## 扩展建议

- 需要鉴权时，在 `app.py` 中为管理页和内部 API 添加 FastAPI `Depends`
- 如果要把 Agent 对话渲染成更友好的时间线，可在管理页前端对 `messages` JSON 做结构化展示
