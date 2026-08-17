-- MySQL 只承载配置、告警和最新信号状态等事务型控制数据；逐笔行情和原始事件
-- 仍保存在 ClickHouse，避免把高吞吐时序写入与管理查询互相影响。

CREATE DATABASE IF NOT EXISTS market
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE market;

CREATE TABLE IF NOT EXISTS data_source (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(120) NOT NULL UNIQUE,
    source_type VARCHAR(40) NOT NULL,
    connection_config JSON NOT NULL DEFAULT (JSON_OBJECT()),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);

CREATE TABLE IF NOT EXISTS symbol_config (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data_source_id BIGINT,
    symbol VARCHAR(80) NOT NULL UNIQUE,
    price_scale INT NOT NULL DEFAULT 8,
    quantity_scale INT NOT NULL DEFAULT 8,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_symbol_config_data_source
        FOREIGN KEY (data_source_id) REFERENCES data_source(id)
);

CREATE TABLE IF NOT EXISTS signal_state (
    symbol VARCHAR(80) NOT NULL,
    signal_name VARCHAR(120) NOT NULL,
    status VARCHAR(30) NOT NULL,
    signal_value DECIMAL(24, 10),
    payload JSON NOT NULL DEFAULT (JSON_OBJECT()),
    event_time DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (symbol, signal_name)
);

CREATE TABLE IF NOT EXISTS service_status (
    service_name VARCHAR(120) PRIMARY KEY,
    status VARCHAR(30) NOT NULL,
    last_heartbeat DATETIME(6) NOT NULL,
    details JSON NOT NULL DEFAULT (JSON_OBJECT())
);

CREATE TABLE IF NOT EXISTS alert_record (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(80),
    alert_type VARCHAR(80) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    payload JSON NOT NULL DEFAULT (JSON_OBJECT()),
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX alert_record_created_at_idx (created_at DESC)
);

CREATE INDEX signal_state_updated_at_idx ON signal_state (updated_at DESC);
