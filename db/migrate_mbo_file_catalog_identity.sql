-- 将现有文件级 (publisher_id, instrument_id) = (0, 0) 目录行展开为身份级目录。
-- 目标表已经使用 ORDER BY (file_sha256, publisher_id, instrument_id)，因此不需要新建 v2 表。
-- 本语句只负责插入新身份行；删除旧的 (0, 0) 行请单独执行
-- delete_replay_identity_zero_rows.sql。
--
-- mbo_rows、时间范围和源顺序边界均从 raw 重新统计，避免把文件级总数复制到每个身份。
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
    raw.first_ts_event,
    raw.last_ts_event,
    raw.min_ts_event,
    raw.max_ts_event,
    raw.first_source_ordinal,
    raw.last_source_ordinal,
    catalog.status,
    now64(3) AS updated_at,
    greatest(catalog.version + 1, toUInt64(toUnixTimestamp64Nano(now64(9)))) AS version
FROM market_data.databento_mbo_file_catalog AS catalog FINAL
INNER JOIN
(
    SELECT
        file_sha256,
        publisher_id,
        instrument_id,
        count() AS mbo_rows,
        argMin(ts_event, source_ordinal) AS first_ts_event,
        argMax(ts_event, source_ordinal) AS last_ts_event,
        min(ts_event) AS min_ts_event,
        max(ts_event) AS max_ts_event,
        min(source_ordinal) AS first_source_ordinal,
        max(source_ordinal) AS last_source_ordinal
    FROM market_data.databento_mbo_raw FINAL
    WHERE publisher_id > 0
      AND instrument_id > 0
    GROUP BY file_sha256, publisher_id, instrument_id
) AS raw USING (file_sha256)
WHERE catalog.publisher_id = 0
  AND catalog.instrument_id = 0;
