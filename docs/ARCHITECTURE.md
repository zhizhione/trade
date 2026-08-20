# 架构

## 系统边界

本项目有两条独立的数据链路，共用同一个 Java L3 订单簿状态机，但不混用输入顺序、存储身份或可用性语义。

```text
历史 DBN
  -> Python 导入器
  -> ClickHouse: raw / import jobs / file catalog
  -> Java 回放服务 + MboBookEngine
  -> REST 目录、WebSocket 回放
  -> React 历史回放页

实时 Kafka / ATAS
  -> Spring Boot 标准化、持久化、MboBookEngine
  -> WebSocket 市场快照
  -> React 实时行情页
```

历史 DBN 不经过 Kafka。Kafka 用于实时缓冲和消费位点，不保存 DBN 文件内容身份、文件内位置或跨文件确定性顺序。

## 模块职责

| 模块 | 职责 | 不负责的内容 |
|---|---|---|
| `python/` | 解码 DBN、计算文件 SHA-256、可恢复导入、目录回填、离线导出与对账 | 推导订单簿、改写 raw 事件语义 |
| `db/` | ClickHouse/MySQL 的标准 schema 与一次性兼容迁移 | 在线状态机或业务映射 |
| `backend/.../orderbook/engine/` | 对单条 MBO 事件执行确定性的 L3 状态转换 | 读取数据库、WebSocket 生命周期 |
| `backend/.../replay/` | 按目录与原始位置回放、预热、采样、分段和播放控制 | 将浏览器快照当作回放输入 |
| `backend/.../adapter/kafka/` | 接收五个实时 topic 并交给领域服务 | 为脏消息提供 DLT 或重试策略 |
| `backend/.../market/processor/` | 标准化实时事件、维护展示快照、隔离失同步 ATAS 流 | 用不完整增量消息猜测 L3 订单簿 |
| `frontend/` | 展示服务端快照、历史回放控制与图表渲染 | 在浏览器中重放 raw MBO 或修复订单簿 |

ClickHouse 保存高频事实和派生分析数据；MySQL 仅保存信号状态和服务心跳等控制数据。实时推送与持久化是解耦的：ClickHouse 写入失败会记录告警，但不会阻止当前事件继续推送。需要可恢复投递时，应在接入层增加可观测的重试或 DLT，不能假设当前实现自动回滚 Kafka offset。

## 历史回放不变量

1. 原始事件唯一身份是 `(file_sha256, source_ordinal)`。`ts_event`、`sequence` 和 `order_id` 都不能替代它。
2. 先按目录的 `trading_date, file_order` 选择文件，再在每个文件内按 `source_ordinal` 读取。时间戳用于筛选，不用于重排。
3. 回放目录键为 `(file_sha256, publisher_id, instrument_id)`；同一 DBN 文件内的不同 Databento 身份不得共享订单簿。
4. `F_LAST` 表示一条 publisher message 的完成边界，但不抑制历史状态机逐条输出；页面采样器仍可按时间桶选择最后状态。
5. 请求窗口前的数据只用于预热。服务端从首个相交文件向前寻找最近的 `R` reset；找不到时拒绝回放，避免输出无法证明完整性的簿。
6. `complete=false` 或 `crossed=true` 的帧只能用于诊断，策略、特征和回测必须显式过滤。
7. 一条 WebSocket 分段最多发送 20,000 个可见时间桶帧。`replay_complete.payload.hasNext=true` 时，客户端必须原样回传 `nextCursor` 给 `replay_continue`；服务端在任务内保留订单簿状态。

`databento_mbo_raw` 是原始事实表。导入任务、目录写入与 raw 提交不构成跨表事务：极端中断后若 raw 已提交但目录缺失，使用 `backfill_mbo_file_catalog.py` 回填目录，不能重新生成或改写 raw 事件身份。

## 实时 L3 与可用性

实时 Kafka topic 为 `market.tick`、`market.trade`、`market.order_book`、`market.mbo` 和 `market.signal`。所有消息先标准化为 `MarketEvent`，之后更新内存快照、尝试持久化并广播 `event` 与 `snapshot` WebSocket envelope。

只有字段完整的 ATAS `market.mbo` 会进入 L3 状态机。最少需要稳定的 `source_stream_id`、严格递增的 `source_sequence`、`canonical_id`、订单 ID、动作；新增和修改还需要买卖方向、价格和正数量。ATAS `Delete` 可以只有订单 ID，统一为删除该活动订单的全部剩余数量；原始 Delete 文本和 payload 仍保留。

状态机按 `(source, source_stream_id)` 隔离实时连接，避免重连后从零开始的序号污染旧簿。历史和实时输入先转换为 `MboBookEngine.BookUpdate`，核心只通过一个 `apply(BookUpdate)` 执行状态迁移；历史和实时工厂都保留 crossed 标记，不因交叉盘自动拒绝；发生序号、订单生命周期或其他簿不变量错误时，服务端会：

1. 关闭并隔离该来源流。
2. 清空这个流关联合约的可见深度。
3. 通过 `snapshot.bookStatus = DESYNCHRONIZED` 广播不可用状态。

前端不得使用该状态下的盘口计算价差、下单或策略信号。恢复必须收到新的完整来源流，或收到明确的 ATAS `Reset`/`Snapshot`/`Clear` 控制事件；普通旧流增量不会解除隔离。未进入 L3 状态机的通用行情消息仍可沿原有展示路径更新深度，但不能被误认为已验证的 ATAS L3 簿。

## 网络契约

```text
GET /api/replay/catalog
GET /api/replay/session
WS  /ws/replay
WS  /ws/market
```

`/api/replay/session` 用于调试和批量检查，交互式历史页面使用 `/ws/replay`。回放命令为 `replay_start`、`replay_play`、`replay_pause`、`replay_speed`、`replay_continue`、`replay_stop`；服务端推送 `replay_ready`、`replay_frame`、`replay_bar`、`replay_complete` 或 `replay_error`。游标里的 `sourceOrdinal` 和 `lastEventNs` 以字符串传输，避免浏览器丢失 64 位整数精度。

`/ws/market` 推送 `event`、`snapshot` 和连接 `status`。`snapshot.bookStatus` 的正常值为 `OK`，失同步时为 `DESYNCHRONIZED`；`snapshot.crossed`/`snapshot.locked` 保留异常盘诊断状态。反向代理或非默认端口部署时，前端通过 `VITE_WS_URL` 和 `VITE_REPLAY_WS_URL` 覆盖 WebSocket 地址，后端通过 `WS_ALLOWED_ORIGINS` 限制允许来源。

## 变更检查清单

修改 raw MBO 字段时，同时更新 ClickHouse schema、`python/import_dbn.py`、Java `MboEvent` 与 ResultSet 映射、回放测试和前端载荷类型。

修改回放命令、游标或限制时，同时更新 WebSocket handler、`frontend/src/realtime/replaySocket.ts`、`frontend/src/domain/replay.ts`、README 和流测试。

修改 ATAS MBO 标准化或失同步规则时，同时更新 `RealtimeMboBookService`、`MarketEventService`、`MarketSnapshot`、实时前端类型与状态展示、`atas_mbo_raw`/`atas_mbo_rejected_raw` 持久化校验和相应测试。任何改变都不能把失同步流的旧深度重新暴露为可交易状态。
