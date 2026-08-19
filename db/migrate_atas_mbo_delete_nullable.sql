-- Existing ClickHouse volumes need this one-time migration before accepting ATAS Delete
-- messages that contain only an exchange_order_id.
ALTER TABLE market_data.atas_mbo_raw MODIFY COLUMN price Nullable(Decimal64(9));
ALTER TABLE market_data.atas_mbo_raw MODIFY COLUMN price_nano Nullable(Int64);
ALTER TABLE market_data.atas_mbo_raw MODIFY COLUMN volume Nullable(UInt64);
