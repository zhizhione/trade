package com.realtime.marketdata.persistence.repository;

import com.realtime.marketdata.core.event.MarketEvent;
import com.realtime.marketdata.market.port.MarketEventStore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;

/**
 * 按来源语义持久化市场事件。原始表不合并来源：Databento MBO 保存文件身份、顺序和官方 14 字段，
 * ATAS 保留其流会话和时间语义；通用展示事件不会落入没有来源身份的兜底表。
 *
 * ClickHouse 写入失败只记录告警并继续实时推送。生产环境若要求数据库失败重试，应在
 * 这里增加可观测的重试/DLT 策略，不能假设当前实现会自动回滚 Kafka offset。
 */
@Repository
public class MarketStorageRepository implements MarketEventStore {

    private static final Logger log = LoggerFactory.getLogger(MarketStorageRepository.class);
    private static final BigDecimal PRICE_NANO_SCALE = new BigDecimal("1000000000");
    private static final long STATUS_UPDATE_INTERVAL_MS = 30_000;

    private final boolean clickHouseEnabled;
    private final String clickHouseUrl;
    private final String clickHouseUsername;
    private final String clickHousePassword;
    private final boolean mysqlEnabled;
    private final String mysqlUrl;
    private final String mysqlUsername;
    private final String mysqlPassword;
    private final AtomicLong nextStatusUpdate = new AtomicLong();

    private HikariDataSource clickHouse;
    private HikariDataSource mysql;

    public MarketStorageRepository(
        @Value("${app.storage.clickhouse.enabled:false}") boolean clickHouseEnabled,
        @Value("${app.storage.clickhouse.url}") String clickHouseUrl,
        @Value("${app.storage.clickhouse.username:market}") String clickHouseUsername,
        @Value("${app.storage.clickhouse.password:}") String clickHousePassword,
        @Value("${app.storage.mysql.enabled:false}") boolean mysqlEnabled,
        @Value("${app.storage.mysql.url}") String mysqlUrl,
        @Value("${app.storage.mysql.username:market}") String mysqlUsername,
        @Value("${app.storage.mysql.password:market}") String mysqlPassword
    ) {
        this.clickHouseEnabled = clickHouseEnabled;
        this.clickHouseUrl = clickHouseUrl;
        this.clickHouseUsername = clickHouseUsername;
        this.clickHousePassword = clickHousePassword;
        this.mysqlEnabled = mysqlEnabled;
        this.mysqlUrl = mysqlUrl;
        this.mysqlUsername = mysqlUsername;
        this.mysqlPassword = mysqlPassword;
    }

    @PostConstruct
    @SuppressWarnings("unused")
    void initialize() {
        if (clickHouseEnabled) {
            clickHouse = dataSource(
                "clickhouse-market-data",
                clickHouseUrl,
                clickHouseUsername,
                clickHousePassword,
                "com.clickhouse.jdbc.ClickHouseDriver"
            );
        }
        if (mysqlEnabled) {
            mysql = dataSource(
                "mysql-market-metadata",
                mysqlUrl,
                mysqlUsername,
                mysqlPassword,
                "com.mysql.cj.jdbc.Driver"
            );
        }
    }

    @Override
    public void save(MarketEvent event) {
        // ClickHouse 是行情事实与分析存储；MySQL 仅保留信号状态和服务心跳等控制数据。
        if (clickHouse != null) {
            saveToClickHouse(event);
        }
        if (mysql != null) {
            if ("signal".equals(event.eventType())) {
                upsertSignalState(event);
            }
            updateServiceStatusIfDue();
        }
    }

    private void saveToClickHouse(MarketEvent event) {
        String source = sourceOf(event);
        if (source == null) {
            log.debug("Skipping ClickHouse persistence for unsupported source on event {}", event.eventId());
            return;
        }

        if ("atas".equals(source)) {
            if ("mbo".equals(event.eventType())) {
                saveAtasMbo(event);
            } else if ("trade".equals(event.eventType())) {
                saveAtasTrade(event);
                saveTrade(event, "atas");
            }
            return;
        }

        if ("databento".equals(source) && "mbo".equals(event.eventType())) {
            saveDatabentoMbo(event);
            if (isTradeAction(event)) {
                saveTrade(event, "databento");
            }
        }
    }

    private void saveAtasMbo(MarketEvent event) {
        UUID streamId = requiredUuid(event, "source_stream_id", "sourceStreamId", "stream_id", "streamId");
        Long canonicalId = event.canonicalId();
        Long sequence = requiredSequence(event);
        BigDecimal price = requiredDecimal(event, "price", "px");
        Long volume = requiredUnsigned(event, "volume", "quantity", "qty", "size");
        if (streamId == null || canonicalId == null || sequence == null || price == null || volume == null) {
            log.warn("Skipping incomplete ATAS MBO event {}: source stream, canonical_id, sequence, price and volume are required", event.eventId());
            return;
        }

        String sql = """
            INSERT INTO market_data.atas_mbo_raw
                (schema_version, source_stream_id, source_sequence, received_utc, event_time_utc,
                 event_time_raw, event_time_kind, canonical_id, root_symbol, contract_symbol,
                 exchange, update_type, side, priority, exchange_order_id, price, price_nano, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        write("ATAS MBO", event, sql, (statement, connection) -> {
            statement.setInt(1, (int) unsignedOrDefault(event, 1, "schema_version", "schemaVersion"));
            statement.setObject(2, streamId);
            statement.setLong(3, sequence);
            statement.setTimestamp(4, Timestamp.from(receivedAt(event)));
            statement.setTimestamp(5, Timestamp.from(event.eventTime()));
            statement.setString(6, eventTimeRaw(event));
            statement.setString(7, eventTimeKind(event));
            statement.setLong(8, canonicalId);
            statement.setString(9, textOrDefault(event, event.symbol(), "root_symbol", "rootSymbol", "root"));
            statement.setString(10, textOrDefault(event, event.symbol(), "contract_symbol", "contractSymbol", "contract"));
            statement.setString(11, textOrDefault(event, "UNKNOWN", "exchange", "venue"));
            statement.setString(12, textOrDefault(event, "Unknown", "update_type", "updateType", "action", "type"));
            statement.setString(13, textOrDefault(event, "Unknown", "side"));
            statement.setLong(14, unsignedOrDefault(event, 0, "priority"));
            statement.setLong(15, unsignedOrDefault(event, 0, "exchange_order_id", "exchangeOrderId", "order_id", "orderId"));
            statement.setBigDecimal(16, price);
            statement.setLong(17, priceNano(event, price));
            statement.setLong(18, volume);
        });
    }

    private void saveAtasTrade(MarketEvent event) {
        UUID streamId = requiredUuid(event, "source_stream_id", "sourceStreamId", "stream_id", "streamId");
        Long canonicalId = event.canonicalId();
        Long sequence = requiredSequence(event);
        BigDecimal price = requiredDecimal(event, "price", "px");
        Long volume = requiredUnsigned(event, "volume", "quantity", "qty", "size");
        if (streamId == null || canonicalId == null || sequence == null || price == null || volume == null) {
            log.warn("Skipping incomplete ATAS trade event {}: source stream, canonical_id, sequence, price and volume are required", event.eventId());
            return;
        }

        String sql = """
            INSERT INTO market_data.atas_trade_raw
                (schema_version, source_stream_id, source_sequence, received_utc, event_time_utc,
                 event_time_raw, event_time_kind, canonical_id, root_symbol, contract_symbol,
                 exchange, direction, data_type, price, price_nano, volume,
                 passive_exchange_order_id, aggressor_exchange_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        write("ATAS trade", event, sql, (statement, connection) -> {
            statement.setInt(1, (int) unsignedOrDefault(event, 1, "schema_version", "schemaVersion"));
            statement.setObject(2, streamId);
            statement.setLong(3, sequence);
            statement.setTimestamp(4, Timestamp.from(receivedAt(event)));
            statement.setTimestamp(5, Timestamp.from(event.eventTime()));
            statement.setString(6, eventTimeRaw(event));
            statement.setString(7, eventTimeKind(event));
            statement.setLong(8, canonicalId);
            statement.setString(9, textOrDefault(event, event.symbol(), "root_symbol", "rootSymbol", "root"));
            statement.setString(10, textOrDefault(event, event.symbol(), "contract_symbol", "contractSymbol", "contract"));
            statement.setString(11, textOrDefault(event, "UNKNOWN", "exchange", "venue"));
            statement.setString(12, aggressorSide(event));
            statement.setString(13, textOrDefault(event, "Trade", "data_type", "dataType"));
            statement.setBigDecimal(14, price);
            statement.setLong(15, priceNano(event, price));
            statement.setLong(16, volume);
            statement.setLong(17, unsignedOrDefault(event, 0, "passive_exchange_order_id", "passiveExchangeOrderId"));
            statement.setLong(18, unsignedOrDefault(event, 0, "aggressor_exchange_order_id", "aggressorExchangeOrderId"));
        });
    }

    private void saveDatabentoMbo(MarketEvent event) {
        // 不从 eventTime、canonicalId 或映射字段派生任何列：这是可重放的官方原始表。
        String fileSha256 = text(event, "file_sha256", "fileSha256");
        Long sourceOrdinal = requiredLong(event, "source_ordinal", "sourceOrdinal");
        Long tsRecv = requiredLong(event, "ts_recv");
        Long tsEvent = requiredLong(event, "ts_event");
        Long rtype = requiredLong(event, "rtype");
        Long publisherId = requiredLong(event, "publisher_id");
        Long instrumentId = requiredLong(event, "instrument_id");
        String action = fixedCharacter(event, "action");
        String side = fixedCharacter(event, "side");
        Long price = nullableLong(event, "price");
        Long size = requiredLong(event, "size");
        Long channelId = requiredLong(event, "channel_id");
        Long orderId = requiredLong(event, "order_id");
        Long flags = requiredLong(event, "flags");
        Long tsInDelta = nullableLong(event, "ts_in_delta");
        Long sequence = requiredLong(event, "sequence");
        if (fileSha256 == null || !fileSha256.matches("[0-9a-f]{64}") || sourceOrdinal == null
            || tsRecv == null || tsEvent == null || rtype == null || publisherId == null || instrumentId == null
            || action == null || side == null || price == null || size == null || channelId == null
            || orderId == null || flags == null || tsInDelta == null || sequence == null) {
            log.warn("Skipping incomplete Databento MBO event {}: file identity, source ordinal and all 14 raw fields are required", event.eventId());
            return;
        }

        String sql = """
            INSERT INTO market_data.databento_mbo_raw
                (file_sha256, source_ordinal, ts_recv, ts_event, rtype, publisher_id, instrument_id, action, side, price,
                 size, channel_id, order_id, flags, ts_in_delta, sequence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        write("Databento MBO", event, sql, (statement, connection) -> {
            statement.setString(1, fileSha256);
            statement.setLong(2, sourceOrdinal);
            statement.setLong(3, tsRecv);
            statement.setLong(4, tsEvent);
            statement.setInt(5, Math.toIntExact(rtype));
            statement.setInt(6, Math.toIntExact(publisherId));
            statement.setLong(7, instrumentId);
            statement.setString(8, action);
            statement.setString(9, side);
            statement.setLong(10, price);
            statement.setLong(11, size);
            statement.setInt(12, Math.toIntExact(channelId));
            statement.setLong(13, orderId);
            statement.setInt(14, Math.toIntExact(flags));
            statement.setInt(15, Math.toIntExact(tsInDelta));
            statement.setLong(16, sequence);
        });
    }

    private void saveTrade(MarketEvent event, String source) {
        // trades 是派生的跨来源视图，必须具有可靠的合约、序列、价格和数量才能写入。
        Long canonicalId = event.canonicalId();
        Long sequence = requiredSequence(event);
        BigDecimal price = requiredDecimal(event, "price", "px");
        Long size = requiredUnsigned(event, "volume", "quantity", "qty", "size");
        if (canonicalId == null || sequence == null || price == null || size == null) {
            log.warn("Skipping incomplete {} normalized trade {}: canonical_id, sequence, price and size are required", source, event.eventId());
            return;
        }

        UUID streamId = optionalUuid(event, "source_stream_id", "sourceStreamId", "stream_id", "streamId");
        Long passiveOrderId = nullableLong(event, "passive_exchange_order_id", "passiveExchangeOrderId", "passive_order_id", "passiveOrderId");
        Long aggressorOrderId = nullableLong(event, "aggressor_exchange_order_id", "aggressorExchangeOrderId", "aggressor_order_id", "aggressorOrderId");
        String sourceEventId = sourceEventId(event, source, sequence, streamId);
        String sql = """
            INSERT INTO market_data.trades
                (source, source_event_id, source_stream_id, source_sequence, canonical_id,
                 ts_event, ts_recv, aggressor_side, price, price_nano, size,
                 passive_order_id, aggressor_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        write("normalized " + source + " trade", event, sql, (statement, connection) -> {
            statement.setString(1, source);
            statement.setString(2, sourceEventId);
            setNullableUuid(statement, 3, streamId);
            statement.setLong(4, sequence);
            statement.setLong(5, canonicalId);
            statement.setTimestamp(6, Timestamp.from(event.eventTime()));
            statement.setTimestamp(7, Timestamp.from(receivedAt(event)));
            statement.setString(8, aggressorSide(event));
            statement.setBigDecimal(9, price);
            statement.setLong(10, priceNano(event, price));
            statement.setLong(11, size);
            setNullableLong(statement, 12, passiveOrderId);
            setNullableLong(statement, 13, aggressorOrderId);
        });
    }

    private boolean isTradeAction(MarketEvent event) {
        String action = text(event, "action");
        return action != null && ("trade".equalsIgnoreCase(action) || "fill".equalsIgnoreCase(action)
            || "t".equalsIgnoreCase(action) || "f".equalsIgnoreCase(action));
    }

    private String sourceOf(MarketEvent event) {
        if (event.source() != null) {
            return event.source();
        }
        String source = text(event, "source", "feed", "provider", "vendor");
        if (source != null) {
            String normalized = source.toLowerCase(Locale.ROOT);
            if (normalized.contains("atas")) {
                return "atas";
            }
            if (normalized.contains("databento")) {
                return "databento";
            }
        }
        if (optionalUuid(event, "source_stream_id", "sourceStreamId", "stream_id", "streamId") != null) {
            return "atas";
        }
        if (node(event, "dataset", "publisher_id", "publisherId", "instrument_id", "instrumentId") != null) {
            return "databento";
        }
        return null;
    }

    private void write(String table, MarketEvent event, String sql, StatementBinder binder) {
        // 每次写入使用独立 JDBC 连接；单条失败不能使内存快照或 WebSocket 推送失效。
        try (Connection connection = clickHouse.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement, connection);
            statement.executeUpdate();
        } catch (SQLException | IllegalArgumentException exception) {
            log.warn("Unable to persist {} event {} to ClickHouse: {}", table, event.eventId(), exception.getMessage());
        }
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement, Connection connection) throws SQLException;
    }

    private JsonNode node(MarketEvent event, String... fields) {
        if (event.data() == null) {
            return null;
        }
        for (String field : fields) {
            JsonNode value = event.data().get(field);
            if (value != null && !value.isNull()) {
                return value;
            }
        }
        return null;
    }

    private String text(MarketEvent event, String... fields) {
        JsonNode value = node(event, fields);
        if (value == null || value.asString().isBlank()) {
            return null;
        }
        return value.asString().trim();
    }

    private String textOrDefault(MarketEvent event, String fallback, String... fields) {
        String value = text(event, fields);
        return value == null ? fallback : value;
    }

    private Long nullableLong(MarketEvent event, String... fields) {
        JsonNode value = node(event, fields);
        if (value == null) {
            return null;
        }
        try {
            if (value.isIntegralNumber()) {
                return value.longValue();
            }
            return Long.valueOf(value.asString());
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private Long requiredLong(MarketEvent event, String... fields) {
        Long value = nullableLong(event, fields);
        return value == null || value < 0 ? null : value;
    }

    private Long requiredSequence(MarketEvent event) {
        Long value = nullableLong(event, "source_sequence", "sourceSequence", "sequence", "seq");
        return value == null || value < 0 ? null : value;
    }

    private String fixedCharacter(MarketEvent event, String field) {
        String value = text(event, field);
        return value != null && value.length() == 1 ? value : null;
    }

    private long unsignedOrDefault(MarketEvent event, long fallback, String... fields) {
        Long value = nullableLong(event, fields);
        return value == null || value < 0 ? fallback : value;
    }

    private Long requiredUnsigned(MarketEvent event, String... fields) {
        return requiredLong(event, fields);
    }

    private BigDecimal decimal(MarketEvent event, String... fields) {
        JsonNode value = node(event, fields);
        if (value == null) {
            return null;
        }
        try {
            return value.isNumber() ? value.decimalValue() : new BigDecimal(value.asString());
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private BigDecimal requiredDecimal(MarketEvent event, String... fields) {
        return decimal(event, fields);
    }

    private UUID optionalUuid(MarketEvent event, String... fields) {
        String value = text(event, fields);
        if (value == null) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private UUID requiredUuid(MarketEvent event, String... fields) {
        return optionalUuid(event, fields);
    }

    private Instant receivedAt(MarketEvent event) {
        JsonNode value = node(event, "ts_recv", "ts_recv_raw", "received_utc", "receivedUtc", "received_at", "receivedAt");
        if (value == null) {
            return event.receivedAt();
        }
        if (value.isNumber()) {
            long epoch = value.longValue();
            return Math.abs(epoch) < 10_000_000_000L
                ? Instant.ofEpochSecond(epoch)
                : Instant.ofEpochMilli(epoch);
        }
        try {
            return Instant.parse(value.asString());
        } catch (DateTimeParseException firstFailure) {
            try {
                return OffsetDateTime.parse(value.asString()).toInstant();
            } catch (DateTimeParseException ignored) {
                return event.receivedAt();
            }
        }
    }

    private String eventTimeRaw(MarketEvent event) {
        return textOrDefault(event, event.eventTime().toString(), "ts_event", "event_time", "eventTime", "timestamp", "ts");
    }

    private String eventTimeKind(MarketEvent event) {
        String kind = text(event, "event_time_kind", "eventTimeKind");
        if (kind != null) {
            return kind;
        }
        String raw = eventTimeRaw(event);
        return raw.endsWith("Z") || raw.matches(".*[+-]\\d{2}:?\\d{2}$") ? "Utc" : "Unspecified";
    }

    private long priceNano(MarketEvent event, BigDecimal price) {
        Long supplied = nullableLong(event, "price_nano", "priceNano");
        if (supplied != null) {
            return supplied;
        }
        return calculatedPriceNano(price);
    }

    private long calculatedPriceNano(BigDecimal price) {
        try {
            return price.multiply(PRICE_NANO_SCALE).setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("price is outside Int64 nanounit range", exception);
        }
    }

    private String aggressorSide(MarketEvent event) {
        String raw = text(event, "direction", "aggressor_side", "aggressorSide", "side");
        if (raw == null) {
            return "Unknown";
        }
        return switch (raw.toUpperCase(Locale.ROOT)) {
            case "BUY", "B" -> "Buy";
            case "SELL", "S" -> "Sell";
            default -> "Unknown";
        };
    }

    private String sourceEventId(MarketEvent event, String source, long sequence, UUID streamId) {
        String id = text(event, "source_event_id", "sourceEventId", "event_id", "eventId", "id");
        if (id != null) {
            return id;
        }
        if (streamId != null) {
            return source + ":" + streamId + ":" + sequence;
        }
        return source + ":" + event.topic() + ":" + sequence;
    }

    private void upsertSignalState(MarketEvent event) {
        String sql = """
            INSERT INTO signal_state
                (symbol, signal_name, status, signal_value, payload, event_time, updated_at)
            VALUES (?, ?, 'ACTIVE', ?, ?, ?, CURRENT_TIMESTAMP(6))
            AS new
            ON DUPLICATE KEY UPDATE
                status = new.status,
                signal_value = new.signal_value,
                payload = new.payload,
                event_time = new.event_time,
                updated_at = CURRENT_TIMESTAMP(6)
            """;
        try (Connection connection = mysql.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.symbol());
            statement.setString(2, event.eventType());
            setNullableDecimal(statement, 3, decimal(event, "signalValue", "signal_value", "value"));
            statement.setString(4, event.data().toString());
            statement.setTimestamp(5, Timestamp.from(event.eventTime()));
            statement.executeUpdate();
        } catch (SQLException exception) {
            log.warn("Unable to update signal state in MySQL: {}", exception.getMessage());
        }
    }

    private void updateServiceStatusIfDue() {
        long now = System.currentTimeMillis();
        long due = nextStatusUpdate.get();
        if (now < due || !nextStatusUpdate.compareAndSet(due, now + STATUS_UPDATE_INTERVAL_MS)) {
            return;
        }

        String sql = """
            INSERT INTO service_status (service_name, status, last_heartbeat, details)
            VALUES ('market-data-backend', 'UP', ?, '{"source":"kafka"}')
            AS new
            ON DUPLICATE KEY UPDATE
                status = new.status,
                last_heartbeat = new.last_heartbeat,
                details = new.details
            """;
        try (Connection connection = mysql.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        } catch (SQLException exception) {
            log.warn("Unable to update backend service status in MySQL: {}", exception.getMessage());
        }
    }

    private HikariDataSource dataSource(
        String poolName,
        String jdbcUrl,
        String username,
        String password,
        String driverClassName
    ) {
        HikariConfig config = new HikariConfig();
        config.setPoolName(poolName);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverClassName);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(2_000);
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }

    private void setNullableLong(PreparedStatement statement, int index, Long value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.BIGINT);
        } else {
            statement.setLong(index, value);
        }
    }

    private void setNullableUuid(PreparedStatement statement, int index, UUID value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.OTHER);
        } else {
            statement.setObject(index, value);
        }
    }

    private void setNullableDecimal(PreparedStatement statement, int index, BigDecimal value)
        throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.DECIMAL);
        } else {
            statement.setBigDecimal(index, value);
        }
    }

    @PreDestroy
    @SuppressWarnings("unused")
    void close() {
        if (clickHouse != null) {
            clickHouse.close();
        }
        if (mysql != null) {
            mysql.close();
        }
    }
}
