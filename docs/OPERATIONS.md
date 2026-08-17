# 运维指南

## 本地启动

```bash
cp .env.example .env
docker compose up -d
./backend/gradlew -p backend bootRun
cd frontend && npm install && npm run dev
```

前端地址为 `http://localhost:5173`；Java API 和 WebSocket 地址为
`http://localhost:8080`。历史回放仅需要 ClickHouse。除非正在测试实时链路或映射链路，
否则 Kafka 和 MySQL 可以保持禁用。

## 验证

```bash
./backend/gradlew -p backend test
python/.venv/bin/python -m unittest discover -s python -p 'test_*.py'
cd frontend && npm run build
```

请通过 `python/.venv/bin/python` 运行 Python 命令，以确保可使用 `databento`、
`clickhouse-connect` 和 `python-dotenv` 依赖。

## 导入与回放检查

1. 大规模导入前，先运行 `import_dbn.py --dry-run --max-records 1000`。
2. 确认 `dbn_import_jobs FINAL` 的状态为 `completed`，且原始行数与
   `committed_rows` 一致。
3. 打开回放页面前，确认 `databento_mbo_file_catalog FINAL` 包含该文件。
4. 确认选中的 `(publisher_id, instrument_id)` 存在于原始数据中。
5. 如果流报告达到 6,000 帧限制，请缩小时间范围。

## Schema 规则

- `db/clickhouse_schema.sql` 是唯一的 ClickHouse 结构定义。
- 新建 Docker 卷会自动执行该文件；已有数据卷缺表或缺文件目录时间边界列时，可在 DBeaver 中重复执行该文件。
- `databento_mbo_file_catalog` 是唯一的 Databento 文件/身份目录，主键为
  `(file_sha256, publisher_id, instrument_id)`，不能再新增平行目录表。
- 该文件不删除或重建原始 MBO 表。现有 raw 表若与当前定义不兼容，应先备份，再从原始 DBN 重新导入。

ClickHouse 删除表操作受大小限制保护。只有在确认目标和备份策略后，
才可在单独会话中临时调整删除保护。

## 故障症状

| 症状 | 检查项 |
|---|---|
| `/api/replay/catalog` 返回 500 | ClickHouse 凭据、数据库结构和 `databento_mbo_file_catalog FINAL` |
| 页面没有帧 | 所选身份标识和请求 ET 时间范围内的原始行 |
| 帧始终不完整 | 请求的文件从订单簿中间开始，或没有 `R/Clear` |
| 没有当前价格 | 帧中没有有效且未交叉的 BBO |
| 时间看起来有偏移 | 查询输入和显示使用纽约时间；数据库值保持 UTC epoch |
| 回放因限制消息停止 | 缩小查询时间范围；浏览器受保护，避免内存无限增长 |
