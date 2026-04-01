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
| 候选梗队列 | `/admin/candidates` | 分页查看全部候选梗，支持清空 |
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

### 候选词管理（内部）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/candidates` | 候选词列表（简表） |
| `GET` | `/api/v1/candidates/page` | 候选词分页列表（完整字段） |
| `DELETE` | `/api/v1/candidates` | 删除全部候选词 |
| `POST` | `/api/v1/candidates/{word}/verify?action=accept\|reject` | 人工审核 |
| `POST` | `/api/v1/memes/{id}/verify?verified=true` | 标记梗为人工验证 |

### 运行记录 / 调度 / Agent 对话

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/runs` | Pipeline 运行记录列表 |
| `GET` | `/api/v1/runs/{run_id}` | 单次运行详情 |
| `GET` | `/api/v1/jobs` | APScheduler 任务概览 |
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
http://localhost:8000/admin/candidates
http://localhost:8000/admin/conversations
```

## 扩展建议

- 需要鉴权时，在 `app.py` 中为管理页和内部 API 添加 FastAPI `Depends`
- 如果要支持手动触发任务，可在 `routes.py` 中增加运行控制路由
- 如果要把 Agent 对话渲染成更友好的时间线，可在管理页前端对 `messages` JSON 做结构化展示
