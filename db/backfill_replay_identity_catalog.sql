-- 将旧的文件级目录展开为文件/Databento 身份级目录。
-- 新导入会由 python/import_dbn.py 直接写入同样的三元组主键。
-- 使用 FINAL 保持与回放读路径相同的去重语义；该查询会扫描 raw 表，请在 ClickHouse 空闲时执行。
-- 旧的 (0, 0) 兼容行不会被删除；回放查询会过滤它，脚本可重复执行。
INSERT INTO market_data.databento_mbo_file_catalog
    (
        file_sha256, publisher_id, instrument_id, source_path, display_name, trading_date,
        file_order, file_size, decoded_rows, mbo_rows, skipped_rows, first_ts_event,
        last_ts_event, min_ts_event, max_ts_event, first_source_ordinal,
        last_source_ordinal, status, updated_at, version
    )
SELECT
    catalog.file_sha256,
    raw.publisher_id,
    raw.instrument_id,
    catalog.source_path,
    catalog.display_name,
    catalog.trading_date,
    catalog.file_order,
    catalog.file_size,
    catalog.decoded_rows,
    raw.mbo_rows,
    catalog.skipped_rows,
    catalog.first_ts_event,
    catalog.last_ts_event,
    catalog.min_ts_event,
    catalog.max_ts_event,
    catalog.first_source_ordinal,
    catalog.last_source_ordinal,
    catalog.status,
    now64(3) AS updated_at,
    greatest(catalog.version + 1, toUInt64(toUnixTimestamp64Nano(now64(9)))) AS version
FROM market_data.databento_mbo_file_catalog AS catalog FINAL
INNER JOIN
(
    SELECT file_sha256, publisher_id, instrument_id, count() AS mbo_rows
    FROM market_data.databento_mbo_raw FINAL
    GROUP BY file_sha256, publisher_id, instrument_id
) AS raw USING (file_sha256)
WHERE catalog.publisher_id = 0
  AND catalog.instrument_id = 0;
