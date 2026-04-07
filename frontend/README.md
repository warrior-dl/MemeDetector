# MemeDetector Frontend

新的管理界面采用 `React + TypeScript + Vite + Ant Design + TanStack Query`，目标是用尽量少的自定义基础设施，把后台调试、审核和梗库查看收口到一个易维护的工作台。

## 页面结构

- `Dashboard`: 查看候选总量、梗库数量和整体运行概况
- `候选工作台`: 筛选候选词，查看来源线索、关联视频、Researcher 对话，并执行人工审核
- `梗库`: 查看已入库词条
- `Pipeline`: 查看调度任务和运行记录

## 当前实现重点

- 路由级懒加载已经启用
- 数据层按 feature 拆分，不再使用单一中央 `hooks.ts`
- 当前 feature hooks 目录：
  - `src/features/dashboard/hooks.ts`
  - `src/features/candidates/hooks.ts`
  - `src/features/library/hooks.ts`
  - `src/features/pipeline/hooks.ts`
- 通用展示组件位于 `src/ui/`
- 通用格式化工具位于 `src/utils/`

## 目录建议

```text
src/
  app/        路由入口与整体壳层
  features/   按业务域拆分的数据 hooks
  pages/      页面组件
  ui/         通用 UI 组件
  utils/      时间/状态等格式化工具
  data/       通用 API 请求与共享类型
```

## 本地开发

```bash
cd frontend
npm install
npm run dev
```

开发服务器默认通过 `vite.config.ts` 把 `/api` 代理到 `http://127.0.0.1:8000`。

## 构建

```bash
cd frontend
npm run build
```

构建产物输出到 `frontend/dist`，后端会把它挂载到 `/`。

## 与后端的关系

- 所有页面都直接复用 FastAPI 的 `/api/v1/*` 接口
- 第一阶段优先复用现有 API，不引入额外 BFF 层
- 前端工作台直接挂在根路径 `/`
- 旧 `/admin` 静态页已移除

## 后端联调

先启动后端：

```bash
python -m meme_detector serve
```

然后访问：

```text
http://127.0.0.1:8000/
```

## 当前约束

- 第一阶段优先复用现有 FastAPI API，不引入额外前端状态框架
- 候选页已经具备筛选、来源线索查看、Researcher 对话查看和人工审核闭环
- `antd` 仍是当前体积最大的依赖族，但现阶段已满足可维护性和可用性要求，不再继续激进拆分
