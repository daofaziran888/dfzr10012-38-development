# 实时流数据处理中心

面向实时数据处理团队的流式计算与管理中心。覆盖流数据源接入、流处理算子编排、窗口计算配置、状态后端管理、检查点恢复、反压监控、延迟追踪与流作业运维的完整实时计算链路。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源接入 | `streamin` | 流数据源 | 源编号、源名称、消息队列 |
| 流处理算子 | `operator` | 算子定义 | 算子编号、算子名称、输入流 |
| 窗口计算配置 | `window` | 窗口定义 | 窗口编号、数据流名称、窗口类型 |
| 状态后端管理 | `state` | 后端配置 | 配置编号、后端类型、存储路径 |
| 检查点恢复 | `checkpoint` | 检查点记录 | 记录编号、流作业名、检查点ID |
| 反压监控面板 | `backpress` | 反压指标 | 指标编号、流作业名、反压级别 |
| 延迟追踪管理 | `latency` | 延迟记录 | 记录编号、流作业名、端到端延迟 |
| 流作业运维 | `jobops` | 作业实例 | 实例编号、作业名称、运行节点 |
| 流数据输出端 | `sink` | 输出配置 | 配置编号、输出类型、目标地址 |
| 流数据格式定义 | `schema` | 格式定义 | 定义编号、流名称、字段名称 |
| 水位线管理 | `watermark` | 水位线配置 | 配置编号、流名称、时间字段 |
| 并行度调优 | `parallel` | 调优记录 | 记录编号、流算子名、当前并行度 |
| 流作业指标采集 | `metric` | 指标采集器 | 采集器编号、作业名称、指标名称 |
| 流告警规则 | `alarm` | 告警规则 | 规则编号、监控指标、告警条件 |
| 自定义函数库 | `udf` | 函数定义 | 函数编号、函数名称、输入参数 |
| 流处理日志中心 | `log` | 日志记录 | 记录编号、流作业名、日志级别 |

## 延迟分段数据结果

延迟追踪记录只保存端到端总耗时、处理延迟、网络延迟与延迟阈值，缺少可归因的分段结果。
`backend/app/services/latency_segments.py` 沿现有数据链路补齐「延迟分段数据结果」：

1. **读取延迟记录**：从 `latency` 模块按记录 ID 取 `端到端延迟`、`处理延迟`、`网络延迟`、`延迟阈值`；
2. **解析耗时字段**：`920` / `920ms` / `0.92s` 统一解析成毫秒整数，非法值直接拒绝；
3. **校验分段耗时**：处理段、网络段取记录值，输出段按「总耗时 - 处理 - 网络」残差补齐（也可显式给 `输出延迟`）；
4. **端到端核对**：`处理段 + 网络段 + 输出段` 必须等于端到端总耗时，否则判为冲突；
5. **幂等快照写入**：按内容摘要判重，重复采样返回同一版本；内容变化版本号 +1 覆盖旧版本；
6. **跨模块消费**：指标采集按作业名读取同一份快照，沿用快照的 `metric_version`（指标采集版本）。

```text
延迟追踪记录 (latency)
   │  读取 端到端延迟 / 处理延迟 / 网络延迟 / 延迟阈值
   ▼
耗时解析与校验 ──非法值──► 拒绝采样（不写快照）
   │
   ▼
三段拆分：处理段 + 网络段 + 显式或残差输出段
   │
   ▼
端到端核对：Σ分段 == 端到端总耗时 ?
   ├─ 否 ──► 判为冲突，不写新快照，沿用已定位版本（消费侧仍只有一条结论）
   └─ 是 ──► 幂等快照（内容摘要判重）
                ├─ 摘要相同 ──► 返回既有版本（幂等）
                └─ 摘要不同 ──► 版本 +1，覆盖旧版本（只保留最新）
                                   │
                                   ├─► GET /api/latency/{id}/segments        延迟追踪页面
                                   └─► GET /api/metric/{id}/latency-result   指标采集关联处理
```

快照保留 `segments`（各段耗时、占比、是否残差、是否超标段）、`over_threshold_ms`、
`over_threshold_segment`、`metric_version`、`version_label` 与 `lineage`（处理经过）。
历史记录缺少处理/网络字段时降级为 `degraded=true`，只保留端到端总耗时，不做三段归因。

接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/latency/{id}/segments` | 采样并幂等写入分段结果，可带采样值覆盖 |
| GET | `/api/latency/{id}/segments` | 读取该记录最新分段结果与冲突提示 |
| GET | `/api/latency/segments` | 列出所有记录的最新分段结果 |
| GET | `/api/metric/{id}/latency-result` | 指标采集关联处理，沿用同一份结果与版本 |

验收样例：LATE-0002（920ms / 处理 600ms / 网络 250ms / 阈值 800ms）拆成
处理 600ms、网络 250ms、输出 70ms 三段，三段合计 920ms 与总耗时一致，标出超标 120ms，
超标段为处理段，指标采集版本 `metric-collect-v1`。

回归测试：`cd backend && .venv/bin/python -I tests/test_latency_segments.py`。

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。
- 同一业务事实只允许有一个共享服务：延迟分段结果由 `services/latency_segments.py`
  持有，其它模块（如指标采集）只读不另算。

领域：数据工程
子方向：数仓与数据湖
