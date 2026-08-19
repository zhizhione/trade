package com.realtime.marketdata.persistence.repository;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplayCursor;
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
          AND coalesce(catalog.min_ts_event, catalog.first_ts_event) IS NOT NULL
          AND coalesce(catalog.max_ts_event, catalog.last_ts_event) IS NOT NULL
        ORDER BY catalog.trading_date NULLS LAST, catalog.file_order, catalog.display_name,
                 catalog.publisher_id, catalog.instrument_id
        """;
    private static final String RAW_EVENTS_SQL = """
        SELECT source_ordinal, ts_recv, ts_event, rtype, publisher_id, instrument_id,
               action, side, price, size, channel_id, order_id, flags, ts_in_delta, sequence
        FROM market_data.databento_mbo_raw FINAL
        PREWHERE file_sha256 = toFixedString(?, 64)
        WHERE publisher_id = ?
          AND instrument_id = ?
          AND ts_event <= ?
        ORDER BY source_ordinal
        """;
    private static final String RAW_EVENTS_AFTER_CURSOR_SQL = """
        SELECT source_ordinal, ts_recv, ts_event, rtype, publisher_id, instrument_id,
               action, side, price, size, channel_id, order_id, flags, ts_in_delta, sequence
        FROM market_data.databento_mbo_raw FINAL
        PREWHERE file_sha256 = toFixedString(?, 64)
        WHERE publisher_id = ?
          AND instrument_id = ?
          AND ts_event <= ?
          AND source_ordinal > ?
        ORDER BY source_ordinal
        """;
    private static final String RESET_ORDINAL_SQL = """
        SELECT minOrNull(source_ordinal)
        FROM market_data.databento_mbo_raw FINAL
        PREWHERE file_sha256 = toFixedString(?, 64)
        WHERE publisher_id = ?
          AND instrument_id = ?
          AND action = 'R'
        """;
    // min/max 在导入时按事件时间计算；first/last 则保留源顺序首尾值，供审计原始文件顺序。
    private static final String FILE_RANGES_SQL = """
        SELECT toString(catalog.file_sha256),
               min(coalesce(catalog.min_ts_event, catalog.first_ts_event)) AS min_ts_event,
               max(coalesce(catalog.max_ts_event, catalog.last_ts_event)) AS max_ts_event,
               max(catalog.decoded_rows) AS decoded_rows,
               max(catalog.last_source_ordinal) AS last_source_ordinal
        FROM market_data.databento_mbo_file_catalog AS catalog FINAL
        WHERE catalog.status = 'completed'
          AND catalog.mbo_rows > 0
          AND catalog.publisher_id = ?
          AND catalog.instrument_id = ?
          AND coalesce(catalog.min_ts_event, catalog.first_ts_event) IS NOT NULL
          AND coalesce(catalog.max_ts_event, catalog.last_ts_event) IS NOT NULL
        GROUP BY catalog.file_sha256
        ORDER BY min(catalog.trading_date) NULLS LAST, min(catalog.file_order),
                 min(catalog.display_name), catalog.file_sha256
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
            log.info("ClickHouse replay storage is disabled");
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
        log.info("ClickHouse replay storage initialized: poolSize={}", config.getMaximumPoolSize());
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
        long startedNanos = System.nanoTime();
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
            List<ReplayCatalogEntry> catalog = List.copyOf(result);
            log.info(
                "Loaded replay catalog: entries={}, elapsedMs={}",
                catalog.size(), elapsedMs(startedNanos)
            );
            return catalog;
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
        return streamEvents(publisherId, instrumentId, startMs, endMs, null, consumer).completed();
    }

    @Override
    public StreamResult streamEvents(
        int publisherId,
        long instrumentId,
        long startMs,
        long endMs,
        ReplayCursor cursor,
        MboEventConsumer consumer
    ) {
        Objects.requireNonNull(consumer, "consumer");
        requireEnabled();
        long startNs = Math.multiplyExact(startMs, 1_000_000L);
        long endNs = Math.multiplyExact(endMs, 1_000_000L);
        long startedNanos = System.nanoTime();
        log.info(
            "Replay event stream started: publisherId={}, instrumentId={}, startMs={}, endMs={}",
            publisherId, instrumentId, startMs, endMs
        );
        try (Connection connection = dataSource.getConnection();
             PreparedStatement rangesStatement = connection.prepareStatement(FILE_RANGES_SQL)) {
            rangesStatement.setInt(1, publisherId);
            rangesStatement.setLong(2, instrumentId);
            List<CatalogRange> catalogRanges = new ArrayList<>();
            try (ResultSet rows = rangesStatement.executeQuery()) {
                while (rows.next()) {
                    long minTsEvent = rows.getLong(2);
                    boolean minWasNull = rows.wasNull();
                    long maxTsEvent = rows.getLong(3);
                    boolean maxWasNull = rows.wasNull();
                    long decodedRows = rows.getLong(4);
                    Long decodedRowsValue = rows.wasNull() ? null : decodedRows;
                    long lastSourceOrdinal = rows.getLong(5);
                    Long lastSourceOrdinalValue = rows.wasNull() ? null : lastSourceOrdinal;
                    if (!minWasNull && !maxWasNull) {
                        catalogRanges.add(new CatalogRange(
                            rows.getString(1).replace("\0", ""),
                            minTsEvent,
                            maxTsEvent,
                            decodedRowsValue,
                            lastSourceOrdinalValue
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
                log.info(
                    "Replay event stream has no overlapping files: publisherId={}, instrumentId={}, elapsedMs={}",
                    publisherId, instrumentId, elapsedMs(startedNanos)
                );
                return new StreamResult(true, null);
            }
            int cursorIndex = cursor == null ? -1 : findCursorIndex(catalogRanges, cursor.fileSha256());
            if (cursor != null && cursorIndex < 0) {
                throw new ReplayDataAccessException("Replay cursor file is no longer available: " + cursor.fileSha256());
            }
            int warmupStart = findWarmupStart(
                connection, catalogRanges, cursorIndex >= 0 ? cursorIndex : firstOverlap, publisherId, instrumentId
            );
            int streamStart = cursorIndex >= 0 ? cursorIndex : warmupStart;
            List<FileRange> ranges = new ArrayList<>();
            for (int index = streamStart; index <= lastOverlap; index++) {
                CatalogRange catalogRange = catalogRanges.get(index);
                ranges.add(new FileRange(
                    catalogRange.fileSha256(), catalogRange.decodedRows(), catalogRange.lastSourceOrdinal()
                ));
            }

            // source_ordinal restarts at zero for every DBN file.  The base is therefore a
            // property of the file's position in the catalog, not of the warmup reset chosen
            // for this particular request.  In particular, a continuation cursor may point at
            // a file containing its own R record; recomputing from warmupStart in that case would
            // assign a smaller ordinal to the continuation events than the first chunk used.
            long ordinalBase = 0;
            for (int index = 0; index < streamStart; index++) {
                ordinalBase += fileOrdinalSpan(catalogRanges.get(index));
            }
            long previousEventNs = cursor == null ? Long.MIN_VALUE : Long.parseLong(cursor.lastEventNs());
            long streamedEvents = 0L;
            for (int rangeIndex = 0; rangeIndex < ranges.size(); rangeIndex++) {
                FileRange range = ranges.get(rangeIndex);
                Long afterSourceOrdinal = cursor != null && rangeIndex == 0
                    ? Long.parseUnsignedLong(cursor.sourceOrdinal())
                    : null;
                FileStreamResult result = streamFile(
                    connection,
                    range,
                    publisherId,
                    instrumentId,
                    endNs,
                    ordinalBase,
                    previousEventNs,
                    afterSourceOrdinal,
                    consumer
                );
                streamedEvents += result.streamedEvents();
                if (!result.exhausted()) {
                    log.info(
                        "Replay event stream stopped by consumer: publisherId={}, instrumentId={}, files={}, events={}, elapsedMs={}",
                        publisherId, instrumentId, ranges.size(), streamedEvents, elapsedMs(startedNanos)
                    );
                    return new StreamResult(
                        false,
                        new ReplayCursor(
                            range.fileSha256(),
                            Long.toUnsignedString(result.maxSourceOrdinal()),
                            Long.toString(result.lastEventNs())
                        )
                    );
                }
                previousEventNs = Math.max(previousEventNs, result.lastEventNs());
                // 磁盘上的 source_ordinal 是 UInt64。Java 以 long 保存其位模式，递增时必须
                // 使用无符号语义；Math.addExact 会错误拒绝最高位为 1 的合法顺序号。
                ordinalBase += fileOrdinalSpan(range, result);
            }
            log.info(
                "Replay event stream complete: publisherId={}, instrumentId={}, files={}, events={}, elapsedMs={}",
                publisherId, instrumentId, ranges.size(), streamedEvents, elapsedMs(startedNanos)
            );
            return new StreamResult(true, null);
        } catch (SQLException exception) {
            log.error(
                "Replay event stream failed: publisherId={}, instrumentId={}, startMs={}, endMs={}, elapsedMs={}",
                publisherId, instrumentId, startMs, endMs, elapsedMs(startedNanos), exception
            );
            throw replayQueryFailure("stream raw MBO replay events", exception);
        }
    }

    private int findCursorIndex(List<CatalogRange> ranges, String fileSha256) {
        for (int index = 0; index < ranges.size(); index++) {
            if (ranges.get(index).fileSha256().equals(fileSha256)) {
                return index;
            }
        }
        return -1;
    }

    private int findWarmupStart(
        Connection connection,
        List<CatalogRange> catalogRanges,
        int firstOverlap,
        int publisherId,
        long instrumentId
    ) throws SQLException {
        try (PreparedStatement resetStatement = connection.prepareStatement(RESET_ORDINAL_SQL)) {
            for (int index = firstOverlap; index >= 0; index--) {
                CatalogRange range = catalogRanges.get(index);
                resetStatement.setString(1, range.fileSha256());
                resetStatement.setInt(2, publisherId);
                resetStatement.setLong(3, instrumentId);
                try (ResultSet rows = resetStatement.executeQuery()) {
                    if (rows.next()) {
                        long resetOrdinal = rows.getLong(1);
                        if (!rows.wasNull()) {
                            log.debug(
                                "Replay warmup reset found: file={}, publisherId={}, instrumentId={}, resetOrdinal={}",
                                shortSha(range.fileSha256()), publisherId, instrumentId,
                                Long.toUnsignedString(resetOrdinal)
                            );
                            return index;
                        }
                    }
                }
            }
        }
        throw new ReplayDataAccessException(
            "Unable to rebuild replay order book: no reset event found for publisherId="
                + publisherId + ", instrumentId=" + instrumentId
        );
    }

    private FileStreamResult streamFile(
        Connection connection,
        FileRange range,
        int publisherId,
        long instrumentId,
        long endNs,
        long ordinalBase,
        long previousEventNs,
        Long afterSourceOrdinal,
        MboEventConsumer consumer
    ) throws SQLException {
        long maxSourceOrdinal = 0L;
        boolean hasSourceOrdinal = false;
        long lastEventNs = previousEventNs;
        long streamedEvents = 0L;
        log.debug(
            "Replay file read started: file={}, publisherId={}, instrumentId={}, ordinalBase={}",
            shortSha(range.fileSha256()), publisherId, instrumentId, ordinalBase
        );
        String sql = afterSourceOrdinal == null ? RAW_EVENTS_SQL : RAW_EVENTS_AFTER_CURSOR_SQL;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, range.fileSha256());
            statement.setInt(2, publisherId);
            statement.setLong(3, instrumentId);
            statement.setLong(4, endNs);
            if (afterSourceOrdinal != null) {
                statement.setLong(5, afterSourceOrdinal);
            }
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
                    lastEventNs = next.tsEventNs();
                    streamedEvents += 1;
                    if (!consumer.accept(next)) {
                        return new FileStreamResult(
                            false, maxSourceOrdinal, hasSourceOrdinal, lastEventNs, streamedEvents
                        );
                    }
                }
            }
        }
        log.debug(
            "Replay file read complete: file={}, publisherId={}, instrumentId={}, events={}, lastSourceOrdinal={}",
            shortSha(range.fileSha256()), publisherId, instrumentId, streamedEvents,
            hasSourceOrdinal ? Long.toUnsignedString(maxSourceOrdinal) : "none"
        );
        return new FileStreamResult(true, maxSourceOrdinal, hasSourceOrdinal, lastEventNs, streamedEvents);
    }

    private long fileOrdinalSpan(CatalogRange range) {
        if (range.decodedRows() != null && range.decodedRows() > 0) {
            return range.decodedRows();
        }
        throw new ReplayDataAccessException(
            "Replay catalog is missing decoded_rows for file " + shortSha(range.fileSha256())
                + "; run the MBO catalog backfill before replay"
        );
    }

    private long fileOrdinalSpan(FileRange range, FileStreamResult result) {
        if (range.decodedRows() != null && range.decodedRows() > 0) {
            return range.decodedRows();
        }
        throw new ReplayDataAccessException(
            "Replay catalog is missing decoded_rows for file " + shortSha(range.fileSha256())
                + "; run the MBO catalog backfill before replay"
        );
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

    private long elapsedMs(long startedNanos) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    private String shortSha(String fileSha256) {
        return fileSha256.length() <= 12 ? fileSha256 : fileSha256.substring(0, 12);
    }

    private record FileRange(
        String fileSha256,
        Long decodedRows,
        Long lastSourceOrdinal
    ) {
    }

    private record CatalogRange(
        String fileSha256,
        long minTsEvent,
        long maxTsEvent,
        Long decodedRows,
        Long lastSourceOrdinal
    ) {
    }

    private record FileStreamResult(
        boolean exhausted,
        long maxSourceOrdinal,
        boolean hasSourceOrdinal,
        long lastEventNs,
        long streamedEvents
    ) {
    }

    @PreDestroy
    @SuppressWarnings("unused")
    void close() {
        if (dataSource != null) {
            log.info("Closing ClickHouse replay storage");
            dataSource.close();
        }
    }
}
