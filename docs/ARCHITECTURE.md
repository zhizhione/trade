# 架构

## 职责划分

本仓库按数据生命周期和正确性要求划分职责：

```text
DBN 文件 -> Python 导入器 -> ClickHouse 原始事实数据
                                           |
                                           v
                                   Java 回放源
                                           |
                                           v
                                   MboBookEngine (L3)
                                           |
                                           v
                             REST 目录 / 回放 WebSocket
                                           |
                                           v
                              React 终端和 DOM 阶梯
```

- `python/` 负责 DBN 解码、文件身份标识、可恢复的导入状态和离线特征实验；不负责推导订单簿，也不改变原始事件语义。
- `backend/.../mbo/model/` 负责规范化 MBO 事件契约，
  `backend/.../orderbook/engine/` 负责确定性的订单簿状态转换。
  历史和实时适配器均调用该状态机。
- `backend/.../replay/` 负责按源顺序回放、采样和流控制；
  `backend/.../persistence/repository/` 提供其 ClickHouse 事件源。
  回放绝不能将前端快照作为输入。
- `backend/.../adapter/kafka/` 负责 Kafka 接入。`market/port/` 和
  `instrument/port/` 是边界层，使处理器不依赖于 WebSocket 和数据库实现。
- `frontend/src/HistoricalReplay.tsx` 负责查询和播放状态；图表组件
  仅渲染传入的状态。
- `db/` 包含 ClickHouse、MySQL 的标准数据库结构，以及可选 NQ 合约种子。

## 回放不变量

1. 原始事件身份为 `(file_sha256, source_ordinal)`。
2. 事件先按文件目录顺序读取，再按 `source_ordinal` 读取。
3. Java 订单簿每次更新一个原始事件，并且仅在 `F_LAST` 时输出。
4. 请求起始时间之前的事件会预热订单簿，但不计入首个可见的
   新增／撤单／成交指标。
5. 从拆分文件中间开始的请求也会先使用目录中的前一文件预热，
   然后仅输出请求时间窗口内的事件。
6. `complete=false` 和 `crossed=true` 帧仅用于诊断，策略代码必须
   排除它们。
7. 单个流分段最多发送 20,000 个可见帧；分段游标由文件哈希、文件内 source ordinal 和上一事件时间组成，客户端通过 `replay_continue` 续播，订单簿状态在服务端任务内保持。

回放目录在 API 边界以 `(file_sha256, publisher_id, instrument_id)` 为键。每个文件中的
全部原始 Databento 身份都会出现在目录中，回放状态机始终按这组身份隔离订单簿。

## 源码布局

`ReplayFrame` 和 `ReplayBar` 等小型 Java 记录仍保留为单独文件，
因为它们是网络传输契约。合并它们会使 REST 和 WebSocket 载荷的边界更难识别。
未使用的遗留 `DepthProfileChart` 已移除；正在使用的图表从 `App.tsx` 或
`HistoricalReplay.tsx` 导入。

## 变更检查清单

修改原始字段时，请同时更新以下内容：

1. `db/clickhouse_schema.sql`。
2. `python/import_dbn.py` 中的列元组和校验和。
3. `MboEvent`、`MboReplayRepository` 的 `ResultSet` 映射。
4. Java 回放测试和前端 TypeScript 载荷类型。

修改回放命令时，请在同一次变更中更新 WebSocket 处理程序、`replay.ts`、
README 中的协议示例和流测试。
