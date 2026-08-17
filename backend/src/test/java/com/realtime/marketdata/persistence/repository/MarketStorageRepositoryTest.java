package com.realtime.marketdata.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.realtime.marketdata.core.event.MarketEvent;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.json.JsonMapper;

class MarketStorageRepositoryTest {

    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void routesAtasMboToAtasMboRaw() throws Exception {
        Capture capture = captureStatements();

        capture.repository().save(event("mbo", """
            {"source":"atas","source_stream_id":"7e5d13a2-f68c-4a98-9fc2-986cba9753d1",
             "source_sequence":1,"canonical_id":1001,"price":23456.25,"volume":7,
             "type":"New","side":"Bid"}
            """));

        assertThat(capture.sql()).singleElement().asString().contains("market_data.atas_mbo_raw");
    }

    @Test
    void routesAtasTradeToRawAndNormalizedTradeTables() throws Exception {
        Capture capture = captureStatements();

        capture.repository().save(event("trade", """
            {"source":"atas","source_stream_id":"7e5d13a2-f68c-4a98-9fc2-986cba9753d1",
             "source_sequence":2,"canonical_id":1001,"price":23456.50,"volume":3,
             "direction":"Buy"}
            """));

        assertThat(capture.sql())
            .anyMatch(sql -> sql.contains("market_data.atas_trade_raw"))
            .anyMatch(sql -> sql.contains("market_data.trades"));
    }

    @Test
    void routesDatabentoMboToDatabentoRaw() throws Exception {
        Capture capture = captureStatements();

        capture.repository().save(event("mbo", """
            {"source":"databento",
             "file_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             "source_ordinal":42,"ts_recv":1704153600000000000,
             "ts_event":1704027604375472715,"rtype":160,"publisher_id":1,
             "instrument_id":750,"action":"A","side":"B","price":17000750000000,
             "size":1,"channel_id":8,"order_id":6849026235350,"flags":40,
             "ts_in_delta":0,"sequence":2305}
            """));

        assertThat(capture.sql()).singleElement().asString().isEqualToIgnoringWhitespace("""
            INSERT INTO market_data.databento_mbo_raw
                (file_sha256, source_ordinal, ts_recv, ts_event, rtype, publisher_id, instrument_id, action, side, price,
                 size, channel_id, order_id, flags, ts_in_delta, sequence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """);
        assertThat(capture.longValues()).isEqualTo(Map.of(
            2, 42L,
            3, 1704153600000000000L,
            4, 1704027604375472715L,
            7, 750L,
            10, 17000750000000L,
            11, 1L,
            13, 6849026235350L,
            16, 2305L
        ));
        assertThat(capture.intValues()).isEqualTo(Map.of(
            5, 160,
            6, 1,
            12, 8,
            14, 40,
            15, 0
        ));
        assertThat(capture.stringValues()).isEqualTo(Map.of(
            1, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            8, "A",
            9, "B"
        ));
    }

    private Capture captureStatements() throws Exception {
        MarketStorageRepository repository = new MarketStorageRepository(
            false,
            "jdbc:ch:http://localhost:8123/market_data",
            "market",
            "unused",
            false,
            "jdbc:mysql://localhost:3306/market",
            "market",
            "unused"
        );
        HikariDataSource dataSource = mock(HikariDataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        ReflectionTestUtils.setField(repository, "clickHouse", dataSource);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sql.capture())).thenReturn(statement);
        ArgumentCaptor<Integer> longIndexes = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Long> longValues = ArgumentCaptor.forClass(Long.class);
        org.mockito.Mockito.doNothing().when(statement).setLong(longIndexes.capture(), longValues.capture());
        ArgumentCaptor<Integer> intIndexes = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> intValues = ArgumentCaptor.forClass(Integer.class);
        org.mockito.Mockito.doNothing().when(statement).setInt(intIndexes.capture(), intValues.capture());
        ArgumentCaptor<Integer> stringIndexes = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<String> stringValues = ArgumentCaptor.forClass(String.class);
        org.mockito.Mockito.doNothing().when(statement).setString(stringIndexes.capture(), stringValues.capture());
        return new Capture(repository, sql, longIndexes, longValues, intIndexes, intValues, stringIndexes, stringValues);
    }

    private MarketEvent event(String eventType, String raw) throws Exception {
        Instant now = Instant.parse("2026-08-09T14:30:00Z");
        var data = json.readTree(raw);
        return new MarketEvent(
            "event-1",
            "market." + eventType,
            eventType,
            data.path("source").asString(),
            data.path("canonical_id").isMissingNode() ? 0L : data.path("canonical_id").longValue(),
            "NQU6",
            now,
            now,
            data.path("sequence").isMissingNode() ? 0L : data.path("sequence").longValue(),
            data.path("price").isMissingNode() ? null : data.path("price").decimalValue(),
            data.path("volume").isMissingNode()
                ? new BigDecimal(data.path("size").asString("0"))
                : data.path("volume").decimalValue(),
            data.path("direction").asString(data.path("side").asString()),
            data
        );
    }

    private record Capture(
        MarketStorageRepository repository,
        ArgumentCaptor<String> statements,
        ArgumentCaptor<Integer> longIndexes,
        ArgumentCaptor<Long> longArguments,
        ArgumentCaptor<Integer> intIndexes,
        ArgumentCaptor<Integer> intArguments,
        ArgumentCaptor<Integer> stringIndexes,
        ArgumentCaptor<String> stringArguments
    ) {
        List<String> sql() {
            return statements.getAllValues();
        }

        Map<Integer, Long> longValues() {
            return zip(longIndexes.getAllValues(), longArguments.getAllValues());
        }

        Map<Integer, Integer> intValues() {
            return zip(intIndexes.getAllValues(), intArguments.getAllValues());
        }

        Map<Integer, String> stringValues() {
            return zip(stringIndexes.getAllValues(), stringArguments.getAllValues());
        }

        private static <T> Map<Integer, T> zip(List<Integer> indexes, List<T> values) {
            java.util.LinkedHashMap<Integer, T> result = new java.util.LinkedHashMap<>();
            for (int index = 0; index < indexes.size(); index++) {
                result.put(indexes.get(index), values.get(index));
            }
            return result;
        }
    }
}
