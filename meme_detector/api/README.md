# api — REST API 模块

基于 FastAPI 提供对外查询接口及内部运营工具。

## 文件

| 文件 | 职责 |
|------|------|
| `app.py`    | FastAPI 应用工厂，注册路由，启动时初始化 Meilisearch 索引 |
| `routes.py` | 所有路由处理函数 |

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
| `GET` | `/api/v1/candidates` | 待审核候选词列表 |
| `POST` | `/api/v1/candidates/{word}/verify?action=accept\|reject` | 人工审核 |
| `POST` | `/api/v1/memes/{id}/verify?verified=true` | 标记梗为人工验证 |

### 统计

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/stats` | 候选词统计 + 梗库总量 |

## 调试

启动后访问自动生成的 Swagger 文档：
```
http://localhost:8000/docs
```

## 扩展建议

- 需要鉴权时，在 `app.py` 中添加 FastAPI `Depends` 中间件
- 需要 Webhook 推送时，在 `routes.py` 中增加 `/webhooks` 路由
