package com.realtime.marketdata.persistence.repository;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.source.ReplayDataAccessException;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * 从 ClickHouse 按顺序流式读取原始 MBO 记录，供请求时重建订单簿。
 *
 * <p>API 边界会按原始 Databento 身份展开文件目录。一个 DBN 文件可能包含多个合约，
 * 而回放状态机绝不能把它们的订单簿事件混在同一条流中。</p>
 */
@Repository
public class MboReplayRepository implements MboReplayEventSource {
    private static final Logger log = LoggerFactory.getLogger(MboReplayRepository.class);
    private static final int CATALOG_BUCKET_MS = 100;
    private static final int FETCH_SIZE = 10_000;
    private static final String FILE_CATALOG_SQL = """
        SELECT toString(catalog.file_sha256),
               coalesce(catalog.min_ts_event, catalog.first_ts_event),
               coalesce(catalog.max_ts_event, catalog.last_ts_event),
               catalog.publisher_id,
               catalog.instrument_id,
               catalog.mbo_rows
        FROM market_data.databento_mbo_file_catalog AS catalog FINAL
        WHERE catalog.status = 'completed'
          AND catalog.mbo_rows > 0
          AND catalog.publisher_id > 0
          AND catalog.instrument_id > 0
          AND catalog.first_ts_event IS NOT NULL
          AND catalog.last_ts_event IS NOT NULL
        ORDER BY catalog.trading_date, catalog.file_order, catalog.display_name,
                 catalog.publisher_id, catalog.instrument_id
        """;
    private static final String RAW_EVENTS_SQL = """
        SELECT source_ordinal, ts_recv, ts_event, rtype, publisher_id, instrument_id,
               action, side, price, size, channel_id, order_id, flags, ts_in_delta, sequence
        FROM market_data.databento_mbo_raw FINAL
        WHERE file_sha256 = toFixedString(?, 64)
          AND publisher_id = ?
          AND instrument_id = ?
          AND ts_event <= ?
        ORDER BY source_ordinal
        """;
    // min/max 在导入时按事件时间计算；first/last 则保留源顺序首尾值，供审计原始文件顺序。
    private static final String FILE_RANGES_SQL = """
        SELECT toString(file_sha256),
               any(coalesce(min_ts_event, first_ts_event)) AS min_ts_event,
               any(coalesce(max_ts_event, last_ts_event)) AS max_ts_event
        FROM market_data.databento_mbo_file_catalog FINAL
        WHERE status = 'completed' AND mbo_rows > 0
          AND publisher_id > 0 AND instrument_id > 0
        GROUP BY file_sha256
        ORDER BY any(trading_date), any(file_order), any(display_name), file_sha256
        """;
    private static final String FILE_STREAM_RANGE_SQL = """
        SELECT count(), max(source_ordinal)
        FROM market_data.databento_mbo_raw FINAL
        WHERE file_sha256 = toFixedString(?, 64)
          AND publisher_id = ?
          AND instrument_id = ?
          AND ts_event <= ?
        """;
    private final boolean enabled;
    private final String url;
    private final String username;
    private final String password;
    private HikariDataSource dataSource;

    public MboReplayRepository(
        @Value("${app.storage.clickhouse.enabled:false}") boolean enabled,
        @Value("${app.storage.clickhouse.url}") String url,
        @Value("${app.storage.clickhouse.username:market}") String username,
        @Value("${app.storage.clickhouse.password:}") String password
    ) {
        this.enabled = enabled;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @PostConstruct
    @SuppressWarnings("unused")
    void initialize() {
        if (!enabled) {
            return;
        }
        HikariConfig config = new HikariConfig();
        config.setPoolName("clickhouse-mbo-replay");
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(3_000);
        config.setInitializationFailTimeout(-1);
        dataSource = new HikariDataSource(config);
    }

    @Override
    public List<ReplayCatalogEntry> catalog() {
        requireEnabled();
        try {
            return catalogOnce();
        } catch (SQLException firstFailure) {
            if (!isConnectionFailure(firstFailure)) {
                throw replayQueryFailure("query MBO replay catalog", firstFailure);
            }
            log.warn("ClickHouse connection reset while querying replay catalog; retrying once: {}", firstFailure.getMessage());
            try {
                return catalogOnce();
            } catch (SQLException retryFailure) {
                throw replayQueryFailure("query MBO replay catalog after retry", retryFailure);
            }
        }
    }

    private List<ReplayCatalogEntry> catalogOnce() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(FILE_CATALOG_SQL);
             ResultSet rows = statement.executeQuery()) {
            List<ReplayCatalogEntry> result = new ArrayList<>();
            while (rows.next()) {
                result.add(catalogEntry(
                    rows.getString(1).replace("\0", ""),
                    rows.getLong(2),
                    rows.getLong(3),
                    rows.getLong(6),
                    rows.getInt(4),
                    rows.getLong(5)
                ));
            }
            return List.copyOf(result);
        }
    }

    private ReplayCatalogEntry catalogEntry(
        String fileSha256,
        long firstTsEvent,
        long lastTsEvent,
        long eventCount,
        int publisherId,
        long instrumentId
    ) {
        return new ReplayCatalogEntry(
            fileSha256,
            publisherId,
            instrumentId,
            symbol(publisherId, instrumentId),
            bucketStartMs(firstTsEvent),
            bucketStartMs(lastTsEvent),
            CATALOG_BUCKET_MS,
            eventCount
        );
    }

    @Override
    public boolean streamEvents(
        int publisherId,
        long instrumentId,
        long startMs,
        long endMs,
        MboEventConsumer consumer
    ) {
        Objects.requireNonNull(consumer, "consumer");
        requireEnabled();
        long startNs = Math.multiplyExact(startMs, 1_000_000L);
        long endNs = Math.multiplyExact(endMs, 1_000_000L);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement rangesStatement = connection.prepareStatement(FILE_RANGES_SQL);
             PreparedStatement streamRangeStatement = connection.prepareStatement(FILE_STREAM_RANGE_SQL)) {
            List<CatalogRange> catalogRanges = new ArrayList<>();
            try (ResultSet rows = rangesStatement.executeQuery()) {
                while (rows.next()) {
                    long minTsEvent = rows.getLong(2);
                    boolean minWasNull = rows.wasNull();
                    long maxTsEvent = rows.getLong(3);
                    if (!minWasNull && !rows.wasNull()) {
                        catalogRanges.add(new CatalogRange(
                            rows.getString(1).replace("\0", ""), minTsEvent, maxTsEvent
                        ));
                    }
                }
            }

            int firstOverlap = -1;
            int lastOverlap = -1;
            for (int index = 0; index < catalogRanges.size(); index++) {
                CatalogRange range = catalogRanges.get(index);
                if (range.maxTsEvent() >= startNs && range.minTsEvent() <= endNs) {
                    if (firstOverlap < 0) firstOverlap = index;
                    lastOverlap = index;
                }
            }
            if (firstOverlap < 0) {
                return true;
            }
            // 将紧邻的前一文件纳入预热段，覆盖从同日拆分文件中间开始的查询；当前文件自身的
            // reset 记录随后会建立完整状态。
            int warmupStart = Math.max(0, firstOverlap - 1);
            List<FileRange> ranges = new ArrayList<>();
            for (int index = warmupStart; index <= lastOverlap; index++) {
                CatalogRange catalogRange = catalogRanges.get(index);
                FileRange range = streamRange(
                    streamRangeStatement, catalogRange.fileSha256(), publisherId, instrumentId, endNs
                );
                if (range != null) {
                    ranges.add(range);
                }
            }

            long ordinalBase = 0;
            long previousEventNs = Long.MIN_VALUE;
            for (FileRange range : ranges) {
                FileStreamResult result = streamFile(
                    connection,
                    range,
                    publisherId,
                    instrumentId,
                    endNs,
                    ordinalBase,
                    previousEventNs,
                    consumer
                );
                if (!result.exhausted()) {
                    return false;
                }
                previousEventNs = Math.max(previousEventNs, result.lastEventNs());
                // 磁盘上的 source_ordinal 是 UInt64。Java 以 long 保存其位模式，递增时必须
                // 使用无符号语义；Math.addExact 会错误拒绝最高位为 1 的合法顺序号。
                long fileEnd = range.lastSourceOrdinal() >= 0
                    ? range.lastSourceOrdinal()
                    : result.maxSourceOrdinal();
                ordinalBase += fileEnd + 1;
            }
            return true;
        } catch (SQLException exception) {
            throw replayQueryFailure("stream raw MBO replay events", exception);
        }
    }

    private FileRange streamRange(
        PreparedStatement statement,
        String fileSha256,
        int publisherId,
        long instrumentId,
        long endNs
    ) throws SQLException {
        statement.setString(1, fileSha256);
        statement.setInt(2, publisherId);
        statement.setLong(3, instrumentId);
        statement.setLong(4, endNs);
        try (ResultSet rows = statement.executeQuery()) {
            if (!rows.next() || rows.getLong(1) == 0) {
                return null;
            }
            return new FileRange(
                fileSha256,
                rows.getLong(2)
            );
        }
    }

    private FileStreamResult streamFile(
        Connection connection,
        FileRange range,
        int publisherId,
        long instrumentId,
        long endNs,
        long ordinalBase,
        long previousEventNs,
        MboEventConsumer consumer
    ) throws SQLException {
        long maxSourceOrdinal = 0L;
        boolean hasSourceOrdinal = false;
        long lastEventNs = previousEventNs;
        try (PreparedStatement statement = connection.prepareStatement(RAW_EVENTS_SQL)) {
            statement.setString(1, range.fileSha256());
            statement.setInt(2, publisherId);
            statement.setLong(3, instrumentId);
            statement.setLong(4, endNs);
            statement.setFetchSize(FETCH_SIZE);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    long sourceOrdinal = rows.getLong("source_ordinal");
                    if (!hasSourceOrdinal
                        || Long.compareUnsigned(sourceOrdinal, maxSourceOrdinal) > 0) {
                        maxSourceOrdinal = sourceOrdinal;
                        hasSourceOrdinal = true;
                    }
                    MboEvent next = event(rows, ordinalBase);
                    // 每个日度 DBN 文件会重复更早时间戳的初始订单簿记录。首文件已建立状态后，
                    // 应忽略这些重复记录，才能让跨文件回放保持时间顺序。
                    if (next.tsEventNs() <= previousEventNs) {
                        continue;
                    }
                    lastEventNs = next.tsEventNs();
                    if (!consumer.accept(next)) {
                        return new FileStreamResult(false, maxSourceOrdinal, lastEventNs);
                    }
                }
            }
        }
        return new FileStreamResult(true, maxSourceOrdinal, lastEventNs);
    }

    @Override
    public String symbol(int publisherId, long instrumentId) {
        return "instrument-" + instrumentId;
    }

    private MboEvent event(ResultSet row, long ordinalBase) throws SQLException {
        return new MboEvent(
            // ClickHouse UInt64 由 Java long 的位模式承载。原始顺序号运算必须在同一 64 位
            // 范围内自然回绕，不能因合法值的最高位为 1 就抛出异常。
            ordinalBase + row.getLong("source_ordinal"),
            row.getLong("ts_recv"),
            row.getLong("ts_event"),
            row.getInt("rtype"),
            row.getInt("publisher_id"),
            row.getLong("instrument_id"),
            oneChar(row.getString("action"), "action"),
            oneChar(row.getString("side"), "side"),
            row.getLong("price"),
            row.getLong("size"),
            row.getInt("channel_id"),
            row.getLong("order_id"),
            row.getInt("flags"),
            row.getInt("ts_in_delta"),
            row.getLong("sequence")
        );
    }

    private long bucketStartMs(long tsEventNs) {
        long eventMs = Math.floorDiv(tsEventNs, 1_000_000L);
        return Math.multiplyExact(Math.floorDiv(eventMs, CATALOG_BUCKET_MS), CATALOG_BUCKET_MS);
    }

    private char oneChar(String value, String name) {
        String clean = value == null ? "" : value.replace("\0", "");
        if (clean.length() != 1) {
            throw new IllegalArgumentException(name + " must contain one character");
        }
        return clean.charAt(0);
    }

    private void requireEnabled() {
        if (dataSource == null) {
            throw new IllegalStateException("ClickHouse storage is disabled");
        }
    }

    private boolean isConnectionFailure(SQLException exception) {
        if (exception.getSQLState() != null && exception.getSQLState().startsWith("08")) {
            return true;
        }
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketException) {
                return true;
            }
        }
        return false;
    }

    private ReplayDataAccessException replayQueryFailure(String operation, SQLException exception) {
        return new ReplayDataAccessException("Unable to " + operation, exception);
    }

    private record FileRange(
        String fileSha256,
        long lastSourceOrdinal
    ) {
    }

    private record CatalogRange(
        String fileSha256,
        long minTsEvent,
        long maxTsEvent
    ) {
    }

    private record FileStreamResult(boolean exhausted, long maxSourceOrdinal, long lastEventNs) {
    }

    @PreDestroy
    @SuppressWarnings("unused")
    void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
