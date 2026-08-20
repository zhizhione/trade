# 运维指南

本指南覆盖本地运行、数据校验和常见故障。完整的接口与数据契约见根目录 `README.md`，职责边界见 `ARCHITECTURE.md`。

## 运行模式

| 场景 | 必需服务 | 推荐配置 |
|---|---|---|
| 历史 DBN 导入与回放 | ClickHouse、后端、前端 | `KAFKA_ENABLED=false`、`MYSQL_ENABLED=false`、`CLICKHOUSE_ENABLED=true` |
| 实时行情展示 | Kafka、后端、前端 | 按是否需要落库启用 ClickHouse/MySQL |
| Kafka/WebSocket 冒烟测试 | Kafka、后端、前端 | `CLICKHOUSE_ENABLED=false`、`MYSQL_ENABLED=false`；可额外启用 `MARKET_SIMULATOR_ENABLED=true` |

Compose 只启动 ClickHouse 与 MySQL；Kafka 需自行提供。根目录 `.env` 会被 Compose、Spring Boot 和 Python 脚本读取。请从 `.env.example` 创建它，并只在 `.env` 或密钥管理系统中保存真实密码和 API key。

## 本地启动

```bash
cp .env.example .env
docker compose up -d
docker compose ps
```

历史回放的最小后端启动方式：

```bash
KAFKA_ENABLED=false ./backend/gradlew -p backend bootRun
```

实时链路冒烟测试可使用内置 `DEMO-USD` 模拟器（必须先提供 Kafka）：

```bash
KAFKA_ENABLED=true MARKET_SIMULATOR_ENABLED=true CLICKHOUSE_ENABLED=false MYSQL_ENABLED=false \
  ./backend/gradlew -p backend bootRun
```

模拟器只用于本地验证，不代表真实行情，也不应写入生产历史数据。

前端：

```bash
cd frontend
npm install
npm run dev
```

默认地址为前端 `http://localhost:5173`、后端 `http://localhost:8080`、ClickHouse HTTP `http://localhost:8123`、ClickHouse Native `localhost:9001`、MySQL `localhost:3306`。端口分别可由 `SERVER_PORT`、`CLICKHOUSE_PORT`、`CLICKHOUSE_NATIVE_PORT` 和 `MYSQL_PORT` 覆盖。前端 Vite 代理只代理 `/api` 到默认后端；WebSocket 使用 `VITE_WS_URL` 与 `VITE_REPLAY_WS_URL` 覆盖。

首次创建 Docker volume 时会执行 `db/clickhouse_schema.sql` 和 `db/mysql_schema.sql`。已有 volume 不会因 `CREATE TABLE IF NOT EXISTS` 自动改变排序键或列类型，升级前必须检查 schema 与相关迁移。

## 验证

```bash
./backend/gradlew -p backend test
python/.venv/bin/python -m unittest discover -s python -p 'test_*.py'
cd frontend && npm run build
```

Python 命令应使用 `python/.venv/bin/python`，确保运行时包含 `databento`、`clickhouse-connect` 和 `python-dotenv`。官方订单簿对账可按 README 中的 `officialOrderBookAudit` Gradle task 和 `validate_mbo_against_official.py` 运行。

## 历史数据检查

1. 大规模导入前运行 `import_dbn.py --dry-run --max-records 1000`；正式导入不得携带 `--max-records`。
2. 确认 `dbn_import_jobs FINAL` 的状态为 `completed`，并核对 `committed_rows`。
3. 确认 `databento_mbo_file_catalog FINAL` 有相同文件和 `(publisher_id, instrument_id)` 的 completed 行。
4. 页面调用 `/api/replay/catalog` 成功后，再发起回放；WebSocket 分段达到 20,000 帧时由前端自动发送 `replay_continue`。
5. 历史和实时都保留 `crossed=true` 供诊断；策略或研究输出仍必须过滤 `complete=false`、`crossed=true`，并将未找到 reset 的请求视为数据不可用，不应补造初始盘口。

快速核对 completed job、raw 和目录行数：

```sql
SELECT jobs.display_name, jobs.committed_rows, raw.raw_rows, catalog.catalog_rows
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
    SELECT file_sha256, sum(mbo_rows) AS catalog_rows
    FROM market_data.databento_mbo_file_catalog FINAL
    WHERE status = 'completed' AND publisher_id > 0 AND instrument_id > 0
    GROUP BY file_sha256
) AS catalog USING (file_sha256)
ORDER BY jobs.display_name;
```

目录缺失而 raw/job 已完成时，运行 `python/backfill_mbo_file_catalog.py` 并提供仍可访问的原 DBN 文件。不要重新导入或手工拼出 `(file_sha256, source_ordinal)`。

## Schema 与迁移

- `db/clickhouse_schema.sql` 是新环境的唯一 ClickHouse schema 定义。
- 旧的文件级 replay catalog 不能直接用于当前按文件/合约身份回放；请备份后按当前
  `db/clickhouse_schema.sql` 重建目录表，再用 `python/backfill_mbo_file_catalog.py` 从原始 DBN 回填。
- 旧 `(0, 0)` 兼容行应在确认新目录行数与 raw 身份一致后，用 ClickHouse `ALTER TABLE ... DELETE` 清理；该操作是异步 mutation，需在 `system.mutations` 检查完成。
- `atas_mbo_raw` 当前 schema 已允许只含订单 ID 的 Delete 使用 Nullable 价格和数量字段。
- raw 表若列或排序键不兼容，先备份，再从原始 DBN 重导；不要通过猜测性 DDL 改写原始 MBO 事实。

ClickHouse 的 `ReplacingMergeTree` 查询当前逻辑状态时使用 `FINAL`。其排序/替换键不是关系数据库的立即 UNIQUE 约束，导入正确性仍依赖文件 SHA、source ordinal 和任务校验。

## 实时 MBO 检查

ATAS 增量 L3 重建要求 `source_stream_id`、递增 `source_sequence`、`canonical_id`、订单 ID 和完整的 Add/Change 字段。`Delete` 允许省略价格和数量，并按活动订单 ID 做全量删除。缺字段的消息会写入 `atas_mbo_rejected_raw`，不会参与可信 L3 重建。

实时页面显示“盘口失同步”或 `snapshot.bookStatus=DESYNCHRONIZED` 时：

1. 在后端日志中查找 `ATAS MBO stream desynchronized`，记录流 ID、源序号和失败原因。
2. 检查采集端是否发送了重复/倒退序号、未知订单，或与原订单不匹配的修改、删除。
3. 让采集端发送完整的 `Reset`/`Snapshot`/`Clear` 控制事件，或结束该 `source_stream_id` 后以新的流 ID 和完整起点重连；服务端不会接受普通旧流增量来恢复该订单簿。
4. 在状态回到 `OK` 前，禁止将空深度、旧缓存或普通 L2 展示深度用于交易决策。

实时持久化失败会记录告警但不会中断当前 WebSocket 推送。生产环境需要对写入失败、Kafka lag、消息解析丢弃和失同步流数量建立监控；当前应用没有 DLT 或自动重试保证。

## 故障症状

| 症状 | 检查项 |
|---|---|
| `/api/replay/catalog` 返回 503 | ClickHouse 是否启用、凭据、schema，以及目录表的 `FINAL` 查询 |
| 页面没有回放帧 | 所选 `(publisher_id, instrument_id)`、纽约时间输入对应的 UTC 范围、raw 是否有事件、是否存在 reset |
| 回放停在分段末尾 | 检查 `replay_complete.payload.hasNext` 和 `nextCursor` 是否被原样传回；查看后端是否记录游标或数据库异常 |
| 帧长期不完整或没有当前价格 | 文件缺少完整起点、没有 BBO，或帧为 crossed；这些帧不能供策略使用 |
| 实时深度消失并显示失同步 | 检查 ATAS 流 ID、序号与订单生命周期；以新的完整流重连 |
| ClickHouse 认证失败 | `.env` 密码是否与已有 volume 内用户一致；修改 `.env` 不会修改已创建的 ClickHouse 用户 |
| MySQL/ClickHouse 端口不可达 | `docker compose ps`、端口覆盖变量，以及应用的 `*_HOST`/`*_PORT` 配置 |
