# Realtime Market Data

面向实时行情和 Databento 历史 MBO 研究的数据平台。项目目前覆盖两条链路：

- 实时链路：Kafka 事件经 Spring Boot 标准化、持久化，并通过 WebSocket 推送到 React 看板。
- 历史链路：Python 无损导入 DBN，Java 回放服务按请求从 raw MBO 流式读取并逐事件重建 L3 订单簿，React 使用 Lightweight Charts 展示 K 线、DOM 最多 400 档和特征。

当前已经完成历史 MBO 的有序唯一存储、单文件订单簿重建和可视化回放。`feature_extractor.py`
仍是 CSV 滚动特征原型；包含排队成交、手续费、滑点和资金曲线的完整策略回测引擎尚未实现。

## 系统架构

```text
历史 Databento DBN
        │
        ▼
Python import_dbn.py
        ├──> databento_mbo_raw          原始 MBO 事实
        ├──> dbn_import_jobs            可恢复导入状态
        └──> databento_mbo_file_catalog 文件目录与跨文件顺序
                       │
                       ▼
       REST 请求 -> Java MboReplayService（调试/批量）
                       │
                       ▼
              Java MboBookEngine ──> React 历史回放（/ws/replay）

实时行情 / 模拟器
        │
        ▼
Kafka: tick / trade / order_book / mbo / signal
        │
        ▼
Spring Boot 标准化 ──────> ClickHouse 高频数据
        │                  MySQL 配置与状态
        ├── market.mbo ──> RealtimeMboBookService
        │                         │
        │                         ▼
        │                  Java MboBookEngine
        │                         │
        ▼
WebSocket 深度快照 /ws/market ──> React 实时看板
```

历史 DBN 导入不经过 Kafka。Kafka 适合实时缓冲，但不适合代替原始文件位置和确定性回放顺序。

## 项目目录

```text
backend/    Java 21 / Spring Boot / MBO 状态机 / 回放 API
frontend/   React / TypeScript / Vite / ECharts
python/     DBN 导入、JSONL 导出、Kafka 离线消费和特征原型
db/         ClickHouse、MySQL schema 和可选 NQ 合约种子
docs/       架构、运维和故障排查说明
compose.yml ClickHouse 与 MySQL 本地环境
```

关键入口：

- `python/import_dbn.py`：正式 DBN 导入器。
- `python/backfill_mbo_file_catalog.py`：为已完成的旧导入补文件目录。
- `backend/.../mbo/MboBookEngine.java`：确定性 L3 MBO 状态机。
- `backend/.../replay/MboReplayService.java`：请求时从 raw 顺序重建回放帧。
- `frontend/src/HistoricalReplay.tsx`：历史盘口回放页面。
- `db/clickhouse_schema.sql`：新数据卷的完整 ClickHouse schema。

详细职责边界见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)，本地启动、验证和故障排查见
[`docs/OPERATIONS.md`](docs/OPERATIONS.md)。

## 环境要求

- Docker Compose
- Java 21
- Node.js 20+
- Python 3.11+
- Kafka，仅实时链路需要；历史 DBN 导入和回放不需要 Kafka

后端已包含 Gradle Wrapper，不需要单独安装 Gradle。命令应从项目根目录执行
`./backend/gradlew -p backend ...`，或进入 `backend/` 后执行 `./gradlew ...`。

## 本地文件边界

源码、数据库 schema、依赖清单和样例配置属于项目；编译结果、依赖缓存、运行日志和导出的
行情文件不属于源码。`.gitignore` 已排除 `backend/build/`、`backend/bin/`、`frontend/dist/`、
`frontend/node_modules/`、`python/.venv/`、`python/__pycache__/`、`tmp/`，以及 `output` 下常见的
CSV、JSONL、Parquet 派生数据。

`tmp/clickhouse/` 可能是手动启动 ClickHouse 时留下的本地数据和诊断日志，不能在不确认服务已停止且
数据可丢弃时删除。Compose 管理的数据库使用 Docker named volumes，也不在项目目录内。

## 从零启动

### 1. 创建本地配置

```bash
cp .env.example .env
```

编辑 `.env`，至少设置启用服务的数据库密码。Compose、Spring Boot 和 Python 脚本都会自动读取
根目录这一个文件，不需要再手工 `source .env`。真实的 `DATABENTO_API_KEY` 只放在已被忽略的
`.env` 或密钥管理系统中，不要写回 `.env.example`。

### 2. 启动数据库

```bash
docker compose up -d
docker compose ps
```

Compose 只在首次创建数据卷时自动执行 `db/clickhouse_schema.sql` 和 `db/mysql_schema.sql`。
已有 ClickHouse 数据卷需要补齐新增表或目录列时，手工重复执行 `db/clickhouse_schema.sql`；
它只创建缺失对象和添加兼容列，不会删除或重建原始数据。

默认端口：

- ClickHouse HTTP：`localhost:8123`
- ClickHouse Native：`localhost:9001`
- MySQL：`localhost:3306`
- 后端：`localhost:8080`
- 前端：`localhost:5173`

### 3. 安装 Python 依赖

```bash
python3 -m venv python/.venv
python/.venv/bin/python -m pip install -r python/requirements.txt
```

### 4. 验证并导入 DBN

先做少量只读解码检查：

```bash
python/.venv/bin/python python/import_dbn.py \
  --dry-run --max-records 1000 \
  /data/glbx-mdp3-20240104.mbo.dbn.zst
```

正式导入必须读取完整文件，禁止搭配 `--max-records`：

```bash
python/.venv/bin/python python/import_dbn.py \
  --batch-size 10000 \
  /data/glbx-mdp3-20240104.mbo.dbn.zst
```

参数可以是文件、目录或 glob。目录会递归查找 `.dbn` 和 `.dbn.zst`，同一绝对路径只处理一次；
真正的跨路径幂等身份是文件内容 SHA-256。

同一交易日被拆成多个文件时，应分别调用并明确目录顺序：

```bash
python/.venv/bin/python python/import_dbn.py --file-order 0 /data/day-part-000.mbo.dbn.zst
python/.venv/bin/python python/import_dbn.py --file-order 1 /data/day-part-001.mbo.dbn.zst
```

标准文件名 `glbx-mdp3-YYYYMMDD.mbo.dbn[.zst]` 会解析出 `trading_date`。非标准文件名保持
`NULL`，不会根据 UTC 事件时间猜交易所业务日。

### 5. 在线回放

先查询已导入文件的 SHA：

```sql
SELECT file_sha256, display_name, committed_rows
FROM market_data.dbn_import_jobs FINAL
WHERE status = 'completed'
ORDER BY completed_at;
```

回放页面先通过 `/api/replay/catalog` 取得可用的合约身份。目录按“文件 × 原始
`(publisher_id, instrument_id)`”展开，因此一个 DBN 文件包含多个合约时会显示全部身份。
用户提交 `startMs`、`endMs` 和 K 线周期后，
前端向 `/ws/replay` 发送 `replay_start`；收到任务就绪消息后自动发送一次 `replay_play`，后端会按
时间范围选择原始文件，从 `databento_mbo_raw FINAL` 按每个文件的 `source_ordinal` 顺序读取 raw MBO，并在
Java 状态机中逐条处理。`F_LAST` 仅用于确认一条 DBN 消息完成；服务端在每个 `bucketMs` 时间桶结束时推送该桶的
最终盘口帧，普通模式每侧最多 100 档，`diagnostic=true` 才允许每侧最多 400 档。回放时钟根据相邻帧的事件时间差和当前倍速推进。

为保证窗口起点的队列状态正确，后端会从首个相交文件向前查找最近的 `R` reset，并从该文件开始处理预热事件；
预热事件不会推送给浏览器。找不到 reset 时会拒绝回放，避免输出不完整订单簿。服务端按前端选定的秒数在线聚合中间价 OHLC；无需再预先返回固定数量的快照或由前端本地计时播放。

在线回放始终按 `source_ordinal` 处理；`ts_event` 回退不会再导致原始事件被丢弃。回放会保留交叉盘并标记 `crossed=true`，便于诊断源数据；前端不把它当作有效当前价，策略和回测必须显式过滤
`!complete` 或 `crossed` 帧。单个 WebSocket 分段最多推送 20,000 个可见时间桶帧；分段结束时服务端返回文件哈希、原始序号和事件时间游标，客户端自动发送 `replay_continue` 继续读取，只有到达请求结束时间才结束整次回放。

### 6. 启动后端

```bash
./backend/gradlew -p backend bootRun
```

纯历史回放不需要 Kafka。只启动回放 API 时可使用：

```bash
KAFKA_ENABLED=false ./backend/gradlew -p backend bootRun
```

只验证 Kafka/WebSocket 而不连接数据库时，可在 `.env` 中设置：

```text
CLICKHOUSE_ENABLED=false
MYSQL_ENABLED=false
```

### 7. 启动前端

```bash
cd frontend
npm install
npm run dev
```

页面入口：

- 实时看板：`http://localhost:5173`
- 历史回放：`http://localhost:5173/?view=replay`

开发服务器会把 `/api/*` 代理到 `localhost:8080`。浏览器只接收已重建的回放帧；HTTP 请求线程从
ClickHouse 流式读取 raw 事件并在 Java 状态机中重放，不把整日 L3 数据一次性加载到内存。

## 数据契约

### `databento_mbo_raw`

主事实表每行由两部分组成：

```text
事件身份: file_sha256, source_ordinal
原始字段: ts_recv, ts_event, rtype, publisher_id, instrument_id,
          action, side, price, size, channel_id, order_id, flags,
          ts_in_delta, sequence
```

`(file_sha256, source_ordinal)` 是稳定事件身份：

- `file_sha256` 是压缩源文件实际字节的 SHA-256，与路径和文件名无关。
- `source_ordinal` 是完整 DBN 解码流中的零基位置，非 MBO 记录也占位置。
- `ts_event` 可能相同或回退，`sequence` 可能按频道/会话重置，`order_id` 也不是事件 ID；它们都不能替代原始位置。

表使用 `ReplacingMergeTree`，逻辑审计和回放查询应使用 `FINAL`。`ORDER BY` 是 ClickHouse
排序/替换键，不等同于关系数据库立即强制的 UNIQUE 约束；导入器还会使用文件级去重令牌和内容校验。

价格保持 Databento `Int64` 固定点整数，1 单位为 `10^-9`。原始层不加入展示符号、浮点价格、
特征或本地展示名称，防止派生逻辑污染可重放事实。

### `dbn_import_jobs`

每次状态变化都插入一个更高 `version` 的完整版本，当前状态必须使用 `FINAL` 查询：

```text
pending -> claimed -> staging -> committing -> completed
                                  └──────────> failed
```

`claim_token` 和 `lease_expires_at` 防止过期 worker 继续提交。`ReplacingMergeTree` 不是事务锁，
因此同一 ClickHouse 集群仍应避免人为同时启动多个进程导入同一文件。

### `databento_mbo_file_catalog`

目录表按 `(file_sha256, publisher_id, instrument_id)` 一行，保存文件元数据和该身份的 MBO 行数；它不保存事件明细。
同一 DBN 文件包含多个合约时，文件元数据会在身份行中重复，回放目录不会混合不同合约的订单簿。

- `trading_date` 只从标准文件名解析。
- `file_order` 是同一交易日多文件的显式顺序，默认 `0`。
- `first_ts_event` / `last_ts_event` 是源顺序第一条/最后一条 MBO 的时间，不是 `min/max`；
  `min_ts_event` / `max_ts_event` 才用于文件与查询时间范围的重叠判断。
- Databento 日文件可能以 snapshot 开始，因此首条事件时间早于文件名日期是正常现象。

全年顺序必须先按目录的 `trading_date, file_order` 选文件，再在每个文件内按 `source_ordinal`。
不能按 `file_sha256`、`ts_event` 或文件路径拼全年时间线。

## DBN 导入逻辑

正式导入一个文件的流程：

```text
计算文件 SHA-256
    -> 领取/续租 dbn_import_jobs
    -> 创建按 SHA 命名的 staging 表
    -> 单遍解码 DBN，分批写 staging 并计算源内容摘要
    -> ClickHouse 端计算 staging 行数和等价摘要
    -> 再次计算文件 SHA，确认导入期间未变化
    -> 使用 file SHA 去重令牌提交 raw
    -> 写文件目录
    -> 标记 job completed
    -> 删除 staging 表
```

内容摘要对每行身份和 14 个原始字段做 SHA-256，再对四段 UInt64 分别求模加和与异或。该聚合与
ClickHouse 返回顺序无关，配合精确行数用于发现漏批、重复批和字段变化，不需要把 staging 全表拉回 Python。

如果服务端已经提交但客户端没有收到确认，重试会使用同一个文件级 token。若异常发生在提交前，任务会
标记为 `failed` 并尽力清理 staging；再次运行同一命令可以重新领取过期或失败任务。相同内容换路径后 SHA
不变，已完成任务会直接跳过。

raw 提交、目录写入和 job 完成不是一个 ClickHouse 跨表事务。极端中断可能导致 raw 已存在而目录缺失；
这时使用目录回填脚本，不要重复构造事件身份。

## MBO 状态机

`MboBookEngine` 以 `(publisher_id, instrument_id)` 隔离订单簿，但所有输入仍必须按文件
`source_ordinal` 严格递增。活动订单由 `order_id` 定位，价位内部用有序队列保留时间优先级。
同一引擎同时接受历史 `MboEvent` 和标准化实时 `LiveMboEvent`；`MboStreamProcessor` 再按
`(source, stream_id)` 隔离连接，避免两个从 `sequence=0` 开始的实时会话混入同一本订单簿。

| Action | 状态变化 |
|---|---|
| `A` | 新增订单；重复活动 `order_id` 直接失败 |
| `M` | 更新已有订单；改价或增量失去优先级，同价减量保留优先级 |
| `C` | 按消息数量部分撤单或全量删除；超出剩余量直接失败 |
| `R` | 只清空当前 publisher/instrument 的订单簿 |
| `T/F/N` | 不改变挂单状态；成交/Fill 对应的簿变化由配套 `C` 表达 |

实时 ATAS 动作映射为 `New -> ADD`、`Change -> MODIFY`、`Delete -> DELETE`。其中历史 Databento
`C` 的 `size` 是撤单量，而 ATAS `Delete` 没有撤单增量语义，状态机直接移除该活动订单的全部剩余量。
如果 ATAS Delete 只提供 `order_id`，状态机也会按活动订单索引删除，不会静默忽略该更新。

只在带 `F_LAST` 的记录后输出状态，避免观察 publisher message 的中间态。输入若带已经派生的
`F_TOB` / `F_MBP` 标志会被拒绝，防止把 BBO/MBP 记录误当 MBO 再重建。

严格模式将交叉盘视为不变量失败。审计模式可以保留交叉盘标记，用于与官方参考数据比对，但策略执行必须
过滤 `!is_complete` 或 `is_crossed=1` 的快照。

## 回放 API 与界面

后端提供：

```text
GET /api/replay/catalog
GET /api/replay/session?publisherId=...&instrumentId=...&bucketMs=100
                        &startMs=...&endMs=...&limit=20000&barIntervalMs=1000&diagnostic=false
WS  /ws/replay
```

`/api/replay/session` 保留给调试和批量检查。页面使用 `/ws/replay`：客户端发送 `replay_start`、
`replay_play`、`replay_pause`、`replay_speed`、`replay_continue`、`replay_stop`；服务端发送 `replay_ready`、
`replay_frame`、`replay_bar`、`replay_complete` 或 `replay_error`。`replay_complete.payload.hasNext=true` 时，
`nextCursor` 必须原样回传给 `replay_continue`；游标中的大整数以字符串传输。

回放页面展示：

- 用户选择 1、5、15、30 或 60 秒周期后，由服务端基于完整、非交叉快照中间价在线生成 K 线。
- DOM 当前买卖盘口（普通模式每侧最多 100 档，诊断模式最多 400 档），包含每档数量和订单数。
- 右侧当前快照的最多 400 档价格深度，以及回放至当前帧的挂单变化提示。
- BBO、400 档深度不平衡、完整性和交叉盘状态。
- 播放、暂停、单步、倍速和已接收帧的时间轴跳转。每段超过 20,000 帧时由客户端自动续播，时间轴只维护当前窗口以避免重复复制完整帧数组。

## 文件目录回填

raw/jobs 已完成，但目录表在之后才创建时：

```bash
docker compose exec -T clickhouse sh -lc \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database market_data' \
  < db/clickhouse_schema.sql

python/.venv/bin/python python/backfill_mbo_file_catalog.py /data/2024-mbo
```

脚本重新读取原始 DBN，只在 `dbn_import_jobs FINAL.status=completed` 且重新解码的 MBO 行数与
`committed_rows` 一致时写目录，不修改 `databento_mbo_raw`。原始 DBN 不可访问时，无法可靠回填
非标准文件名的交易日和原始首尾位置。

同日分片可逐文件配合 `--file-order N` 回填。

已有旧文件级目录的数据卷，确认目录表已使用
`ORDER BY (file_sha256, publisher_id, instrument_id)` 后，先执行以下身份迁移语句：

```bash
docker compose exec -T clickhouse sh -lc \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database market_data' \
  < db/migrate_mbo_file_catalog_identity.sql
```

脚本会从 raw 表重新统计每个文件/身份的行数和时间边界。完成校验后，再单独执行：

```bash
docker compose exec -T clickhouse sh -lc \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database market_data' \
  < db/delete_replay_identity_zero_rows.sql
```

删除语句会清理旧的 `(publisher_id, instrument_id) = (0, 0)` 兼容行。

迁移后、删除前先确认每个 raw 身份都有等行数的目录记录：

```sql
SELECT raw.file_sha256, raw.publisher_id, raw.instrument_id,
       raw.mbo_rows AS raw_mbo_rows, catalog.mbo_rows AS catalog_mbo_rows
FROM
(
    SELECT file_sha256, publisher_id, instrument_id, count() AS mbo_rows
    FROM market_data.databento_mbo_raw FINAL
    WHERE publisher_id > 0 AND instrument_id > 0
    GROUP BY file_sha256, publisher_id, instrument_id
) AS raw
LEFT JOIN
(
    SELECT file_sha256, publisher_id, instrument_id, mbo_rows
    FROM market_data.databento_mbo_file_catalog FINAL
    WHERE status = 'completed' AND publisher_id > 0 AND instrument_id > 0
) AS catalog USING (file_sha256, publisher_id, instrument_id)
WHERE catalog.mbo_rows IS NULL OR raw.mbo_rows != catalog.mbo_rows;
```

该查询必须返回零行。执行清理脚本后，以下查询必须返回 `0`：

```sql
SELECT count()
FROM market_data.databento_mbo_file_catalog FINAL
WHERE publisher_id = 0 AND instrument_id = 0;
```

## 常用验证 SQL

查看导入状态：

```sql
SELECT display_name, status, attempt, expected_rows, staged_rows, committed_rows,
       error_message, updated_at, completed_at
FROM market_data.dbn_import_jobs FINAL
ORDER BY updated_at DESC;
```

查看文件目录：

```sql
SELECT trading_date, file_order, display_name, file_sha256, publisher_id, instrument_id,
       mbo_rows, min_ts_event, max_ts_event
FROM market_data.databento_mbo_file_catalog FINAL
WHERE status = 'completed'
ORDER BY trading_date NULLS LAST, file_order, display_name;
```

检查物理行数和事件身份数：

```sql
SELECT
    count() AS physical_rows,
    uniqExact(tuple(file_sha256, source_ordinal)) AS unique_events
FROM market_data.databento_mbo_raw;
```

按文件核对 raw、jobs 和 catalog 行数：

```sql
SELECT jobs.display_name, jobs.file_sha256, jobs.committed_rows,
       raw.raw_rows, catalog.mbo_rows,
       jobs.committed_rows = raw.raw_rows AS raw_ok,
       jobs.committed_rows = catalog.mbo_rows AS catalog_ok
FROM
(
    SELECT file_sha256, display_name, committed_rows
    FROM market_data.dbn_import_jobs FINAL
    WHERE status = 'completed'
) AS jobs
LEFT JOIN
(
    SELECT file_sha256, count() AS raw_rows
    FROM market_data.databento_mbo_raw FINAL
    GROUP BY file_sha256
) AS raw USING (file_sha256)
LEFT JOIN
(
    SELECT file_sha256, sum(mbo_rows) AS mbo_rows
    FROM market_data.databento_mbo_file_catalog FINAL
    WHERE status = 'completed' AND publisher_id > 0 AND instrument_id > 0
    GROUP BY file_sha256
) AS catalog USING (file_sha256)
ORDER BY jobs.display_name;
```

查询一个文件的确定性事件流：

```sql
SELECT *
FROM market_data.databento_mbo_raw FINAL
WHERE file_sha256 = '<64-char-sha256>'
ORDER BY source_ordinal;
```

## 数据库 schema

`db/clickhouse_schema.sql` 是 ClickHouse 的唯一结构定义。新数据卷会由 Compose 自动初始化；
已有数据卷执行前必须先用 `SHOW CREATE TABLE market_data.databento_mbo_file_catalog` 确认
`PRIMARY KEY file_sha256` 和 `ORDER BY (file_sha256, publisher_id, instrument_id)` 已存在。
`CREATE TABLE IF NOT EXISTS` 不会修改旧表的排序键；旧文件级目录应按上面的迁移、校验、清理步骤处理。

如果现有 `databento_mbo_raw` 的列或排序键不符合该文件定义，不能用 DDL 就地猜测或转换原始
MBO 记录。请先备份现有表，再从原始 DBN 使用 `python/import_dbn.py` 导入到符合当前 schema 的库。

## 实时链路与合约身份

Kafka topics：

- `market.tick`
- `market.trade`
- `market.order_book`
- `market.mbo`
- `market.signal`

后端向前端发送两类 WebSocket envelope：

```json
{"type":"event","payload":{"eventType":"trade","symbol":"BTC-USD"}}
{"type":"snapshot","payload":{"symbol":"BTC-USD","lastPrice":64250.25,"bids":[],"asks":[]}}
```

实时原始事件保留上游身份，不再查询本地来源映射表。Databento 使用事件中的
`instrument_id` 作为稳定数值身份；ATAS 写入原始表和派生表时必须明确提供
`canonical_id`，缺失时仍可展示但会跳过需要该字段的持久化记录。

字段完整的 ATAS `market.mbo` 不再直接把输入价格当作盘口快照：后端使用
`source_stream_id + source_sequence + canonical_id + exchange_order_id + side + price + volume`
构造订单级事件，经 Java 状态机重建后再推送完整深度。价格用 `BigDecimal` 精确换算为纳米整数；
MBO 挂单价不会覆盖 `lastPrice`。缺少稳定流 ID、源序号或订单字段的消息只保留原有通用展示路径，
不会进入实时 L3 状态机。

离线 `import_dbn.py` 始终保留 DBN 原始数字 `instrument_id`。Java 回放 API 使用
`instrument-<id>` 作为展示名称，不影响订单簿重建。

加载可选的 NQ 合约种子：

```bash
docker compose exec -T clickhouse sh -lc \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database market_data' \
  < db/seed_nq_instruments.sql
```

## Python 辅助工具

导出 DBN 为保持整数精度和源顺序的 JSON Lines：

```bash
python/.venv/bin/python python/export_mbo_json.py \
  /data/glbx-mdp3-20240104.mbo.dbn.zst \
  --output output/glbx-mdp3-20240104.mbo.jsonl
```

提取 DBN 中最先出现的 instrument IDs：

```bash
python/.venv/bin/python python/extract_dbn_instrument_ids.py /data/2024-mbo
```

消费实时 Kafka 到 CSV，再计算滚动特征原型：

```bash
python/.venv/bin/python python/kafka_consumer.py --csv output/events.csv
python/.venv/bin/python python/feature_extractor.py \
  output/events.csv output/features.csv --window 50
```

该特征脚本按 `symbol` 隔离滚动窗口，目前包括收益、波动率、signed quantity、订单流 z-score
和动量。它不是基于完整 L3 队列的生产特征管线。

## 测试

Python：

```bash
python/.venv/bin/python -m unittest discover -s python -p 'test_*.py'
```

Java：

```bash
./backend/gradlew -p backend test
```

前端：

```bash
cd frontend
npm run build
```

## 常见问题

### ClickHouse 认证失败

确认 `.env` 中 `CLICKHOUSE_USERNAME` / `CLICKHOUSE_PASSWORD` 与容器创建时一致。修改 `.env`
不会自动修改已有 ClickHouse 用户；需要使用原凭据更新用户，或在确认可丢弃数据后重建 volume。

### 大表无法 DROP，错误码 359

ClickHouse 的大表删除保护生效。只有明确选择不可恢复的重建路径时，在同一客户端会话先执行：

```sql
SET max_table_size_to_drop = 0;
DROP TABLE market_data.databento_mbo_raw SYNC;
```

不要为了方便长期关闭服务器级保护。若确需删除大表，请在已确认目标和备份策略的单独会话中临时设置。

### job 已 completed，但目录为空

重新运行 `import_dbn.py` 会因 completed job 跳过文件。目录表创建并补齐时间边界列后，使用
`backfill_mbo_file_catalog.py` 从原始 DBN 回填。

### 首条事件时间不属于文件名日期

日文件可能带前置 snapshot；目录中的首尾时间按源文件位置记录，不是交易日边界或 min/max。交易日以
标准文件名和交易所业务规则为准。

### 回放页面显示“暂无回放数据”

依次检查：ClickHouse 存储是否启用、raw 文件是否完成、`databento_mbo_raw FINAL` 是否有对应
`file_sha256/publisher_id/instrument_id` 行，以及后端 `/api/replay/catalog` 是否可访问。

### 回放快照长期处于“预热中”

只有看到当前 `(publisher_id, instrument_id)` 的第一次 `R/Clear` 后才标记完整。文件从中段截取、
缺少起始 snapshot 或数据损坏时，预热状态不能用于策略执行。

### `./gradle` 或 `./gradlew` 不存在

项目 wrapper 位于 `backend/gradlew`。从根目录使用 `./backend/gradlew -p backend test`，进入
`backend/` 后使用 `./gradlew test`。

## 后续开发边界

建议保持以下分层：raw 永远无损且可重放；状态机只负责确定性订单簿；特征层从带质量标记的快照派生；
回测层独立建模信号时点、下单延迟、队列位置、部分成交、手续费和滑点。不要把特征或策略状态写回 raw。

优先级建议：

1. 增加按文件目录批量构建全年快照的 manifest 和断点恢复。
2. 从 L3/L2 快照派生 BBO、spread、microprice、imbalance、OFI、撤单率和队列特征。
3. 建立特征注册表、版本、窗口和数据质量 lineage。
4. 实现事件驱动回测器，并强制过滤 incomplete/crossed/缺口窗口。
5. 用官方 MBP/BBO 数据做 `F_LAST` 边界差异验证，再扩大到全年策略研究。当前可用独立校验器复跑同日对账：
   `python/.venv/bin/python python/validate_mbo_against_official.py --mbo <同日.mbo.dbn.zst> --mbp10 <同日.mbp-10.dbn.zst> --tbbo <同日.tbbo.dbn.zst>`。
   对齐键为 `(sequence, ts_event)`，并按 `action + price + size` 处理 MBP-10 对十档外 MBO 事件的省略。
