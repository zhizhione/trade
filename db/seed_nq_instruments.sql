-- GLBX.MDP3 数据集中的 NQ Databento 身份种子。这里按项目当前 publisher_id=1 的约定
-- 初始化；切换数据集或发布者前，必须先以实际 DBN 文件核对 publisher_id 与 instrument_id。
--
-- 本种子集刻意让 canonical_id 与已知 Databento ID 相同，便于首次导入核对。实时来源
-- 保留上游提供的身份；Databento 使用 instrument_id，ATAS 需明确提供 canonical_id。

INSERT INTO market_data.instruments
    (canonical_id, root_symbol, contract_symbol, exchange, expiry_date,
     databento_dataset, databento_publisher_id, databento_instrument_id, atas_instrument,
     tick_size_nano, contract_multiplier, currency, exchange_timezone, is_active, version)
VALUES
    (750, 'NQ', 'NQH4', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (13743, 'NQ', 'NQM4', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (4358, 'NQ', 'NQU4', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (106364, 'NQ', 'NQZ4', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (42288528, 'NQ', 'NQH5', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (42005804, 'NQ', 'NQM5', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (42008487, 'NQ', 'NQU5', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (158704, 'NQ', 'NQZ5', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (42002475, 'NQ', 'NQH6', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (42004058, 'NQ', 'NQM6', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1),
    (42004177, 'NQ', 'NQU6', 'CME', NULL, '', NULL, NULL, NULL, 250000000, 20, 'USD', 'America/Chicago', 1, 1);
