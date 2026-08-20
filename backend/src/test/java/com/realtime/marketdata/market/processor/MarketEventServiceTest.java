package com.realtime.marketdata.market.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.realtime.marketdata.adapter.web.market.MarketWebSocketHandler;
import com.realtime.marketdata.core.event.MarketEvent;
import com.realtime.marketdata.marketstate.model.MarketSnapshot;
import com.realtime.marketdata.persistence.repository.MarketStorageRepository;
import java.time.Instant;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.json.JsonMapper;

class MarketEventServiceTest {

    @Test
    void normalizesAndBroadcastsAnEvent() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );

        MarketEvent event = service.process(
            "market.trade",
            """
                {
                  "symbol": "btc-usd",
                  "timestamp": "2026-08-09T09:30:00Z",
                  "price": 64250.25,
                  "qty": 0.4,
                  "side": "buy"
                }
                """
        );

        assertThat(event.eventType()).isEqualTo("trade");
        assertThat(event.symbol()).isEqualTo("BTC-USD");
        assertThat(event.price()).isEqualByComparingTo("64250.25");
        assertThat(event.side()).isEqualTo("BUY");
        verify(storage).save(event);
        verify(socket).broadcastEvent(event);
    }

    @Test
    void parsesNumericNanosecondEventTimesWithoutTreatingThemAsMilliseconds() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );

        MarketEvent event = service.process(
            "market.trade",
            "{\"symbol\":\"NQ\",\"timestamp\":1723213800123456789,\"price\":20000,\"qty\":1}"
        );

        assertThat(event.eventTime()).isEqualTo(Instant.parse("2024-08-09T14:30:00.123456789Z"));
    }

    @Test
    void normalizesAtasFieldsAndTreatsNaiveEventTimeAsConfiguredExchangeTime() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );

        MarketEvent event = service.process(
            "market.trade",
            """
                {
                  "source": "atas",
                  "source_stream_id": "7e5d13a2-f68c-4a98-9fc2-986cba9753d1",
                  "event_time": "2026-08-09T09:30:00.1234567",
                  "canonical_id": 42004177,
                  "symbol": "NQU6",
                  "price": 23456.25,
                  "volume": 7,
                  "direction": "Buy"
                }
                """
        );

        assertThat(event.eventTime()).isEqualTo(Instant.parse("2026-08-09T14:30:00.123456700Z"));
        assertThat(event.quantity()).isEqualByComparingTo("7");
        assertThat(event.side()).isEqualTo("BUY");
        assertThat(event.canonicalId()).isEqualTo(42004177L);
        assertThat(event.symbol()).isEqualTo("NQU6");
    }

    @ParameterizedTest
    @ValueSource(strings = {"New", "Change", "Delete"})
    void doesNotTreatAtasMboNewChangeOrDeleteVolumeAsOrderFlow(String action) throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );

        service.process(
            "market.mbo",
            """
                {"source":"atas","event_time":"2026-08-09T09:30:00",
                 "symbol":"NQU6","type":"%s","side":"Bid","volume":10}
                """.formatted(action)
        );

        org.mockito.ArgumentCaptor<MarketSnapshot> snapshots =
            org.mockito.ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket).broadcastSnapshot(snapshots.capture());
        assertThat(snapshots.getValue().lastEventType()).isEqualTo("mbo");
        assertThat(snapshots.getValue().orderFlow()).isEqualByComparingTo("0");
    }

    @Test
    void retainsIncompleteAtasMboWithoutUsingItsAggregateDepthPayload() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );

        service.process("market.mbo", """
            {"source":"atas","type":"New","symbol":"NQU6",
             "bids":[[23456.25,10]],"asks":[[23456.50,8]]}
            """);

        ArgumentCaptor<MarketSnapshot> snapshots = ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket).broadcastSnapshot(snapshots.capture());
        assertThat(snapshots.getValue().bids()).isEmpty();
        assertThat(snapshots.getValue().asks()).isEmpty();
    }

    @Test
    void rebuildsLiveAtasDepthFromOrderLevelUpdates() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );
        String stream = "7e5d13a2-f68c-4a98-9fc2-986cba9753d1";

        service.process("market.mbo", atasMbo(stream, 0, "New", "Bid", "23456.25", 10, 101));
        service.process("market.mbo", atasMbo(stream, 1, "New", "Ask", "23456.50", 8, 201));
        service.process("market.mbo", atasMbo(stream, 2, "Change", "Bid", "23456.25", 6, 101));
        service.process("market.mbo", atasMbo(stream, 3, "Delete", "Ask", "23456.50", 0, 201));

        ArgumentCaptor<MarketSnapshot> snapshots = ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket, times(4)).broadcastSnapshot(snapshots.capture());
        MarketSnapshot added = snapshots.getAllValues().get(1);
        MarketSnapshot changed = snapshots.getAllValues().get(2);
        MarketSnapshot deleted = snapshots.getAllValues().get(3);

        assertThat(added.bids()).containsExactly(
            new MarketSnapshot.DepthLevel(new java.math.BigDecimal("23456.250000000"), new java.math.BigDecimal("10"))
        );
        assertThat(added.asks()).containsExactly(
            new MarketSnapshot.DepthLevel(new java.math.BigDecimal("23456.500000000"), new java.math.BigDecimal("8"))
        );
        assertThat(changed.bids()).containsExactly(
            new MarketSnapshot.DepthLevel(new java.math.BigDecimal("23456.250000000"), new java.math.BigDecimal("6"))
        );
        assertThat(deleted.asks()).isEmpty();
        assertThat(deleted.lastPrice()).isNull();
    }

    @Test
    void deletesAtasOrderByIdWhenDeleteOmitsSideAndPrice() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );
        String stream = "delete-without-price";

        service.process("market.mbo", """
            {"source":"atas","source_stream_id":"%s","source_sequence":0,
             "event_time":"2026-08-09T09:30:00","type":"New","side":"Ask",
             "price":23456.50,"volume":8,"exchange_order_id":201,"canonical_id":42004177}
            """.formatted(stream));
        service.process("market.mbo", """
            {"source":"atas","source_stream_id":"%s","source_sequence":1,
             "event_time":"2026-08-09T09:30:01","type":"Delete",
             "exchange_order_id":201,"canonical_id":42004177}
            """.formatted(stream));

        ArgumentCaptor<MarketSnapshot> snapshots = ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket, times(2)).broadcastSnapshot(snapshots.capture());
        assertThat(snapshots.getAllValues().getLast().asks()).isEmpty();
    }

    @Test
    void dropsADesynchronizedAtasStreamAndDoesNotPoisonKafkaProcessing() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        RealtimeMboBookService realtime = new RealtimeMboBookService();
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            realtime,
            "America/Chicago"
        );
        String stream = "desync-stream";

        service.process("market.mbo", atasMbo(stream, 0, "New", "Bid", "23456.25", 10, 101));
        // Reusing a source sequence is an unrecoverable incremental-book error. The stream is
        // quarantined until an explicit provider reconnect/reset is observed.
        service.process("market.mbo", atasMbo(stream, 0, "New", "Bid", "23456.00", 4, 102));
        service.process("market.mbo", atasMbo(stream, 1, "New", "Ask", "23456.50", 8, 201));
        realtime.closeStream(stream);
        service.process("market.mbo", atasMbo(stream, 0, "New", "Ask", "23456.50", 8, 201));

        ArgumentCaptor<MarketSnapshot> snapshots = ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket, times(4)).broadcastSnapshot(snapshots.capture());
        assertThat(snapshots.getAllValues().get(1).bids()).isEmpty();
        assertThat(snapshots.getAllValues().getLast().bids()).isEmpty();
        assertThat(snapshots.getAllValues().getLast().asks())
            .containsExactly(new MarketSnapshot.DepthLevel(
                new java.math.BigDecimal("23456.500000000"), new java.math.BigDecimal("8")
            ));
    }

    @Test
    void acceptsAnExplicitResetToRecoverAQuarantinedStream() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        RealtimeMboBookService realtime = new RealtimeMboBookService();
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(), storage, socket, realtime, "America/Chicago"
        );
        String stream = "reset-stream";
        service.process("market.mbo", atasMbo(stream, 0, "New", "Bid", "23456.25", 10, 101));
        service.process("market.mbo", atasMbo(stream, 0, "New", "Bid", "23456.00", 4, 102));
        service.process("market.mbo", """
            {"source":"atas","source_stream_id":"%s","source_sequence":1,
             "event_time":"2026-08-09T09:30:01","type":"Reset","canonical_id":42004177}
            """.formatted(stream));
        service.process("market.mbo", atasMbo(stream, 2, "New", "Ask", "23456.50", 8, 201));

        ArgumentCaptor<MarketSnapshot> snapshots = ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket, times(4)).broadcastSnapshot(snapshots.capture());
        assertThat(snapshots.getAllValues().get(2).bookStatus()).isEqualTo("OK");
        assertThat(snapshots.getAllValues().getLast().asks()).containsExactly(
            new MarketSnapshot.DepthLevel(new java.math.BigDecimal("23456.500000000"), new java.math.BigDecimal("8"))
        );
        verify(storage, times(4)).save(org.mockito.ArgumentMatchers.any(MarketEvent.class));
    }

    @Test
    void limitsNonMboDepthSnapshotsToFourHundredLevels() throws Exception {
        MarketStorageRepository storage = mock(MarketStorageRepository.class);
        MarketWebSocketHandler socket = mock(MarketWebSocketHandler.class);
        MarketEventService service = new MarketEventService(
            JsonMapper.builder().findAndAddModules().build(),
            storage,
            socket,
            new RealtimeMboBookService(),
            "America/Chicago"
        );
        String levels = IntStream.range(0, 450)
            .mapToObj(level -> "[%d,%d]".formatted(100 - level, level + 1))
            .collect(java.util.stream.Collectors.joining(","));

        service.process("market.order_book", "{\"symbol\":\"NQ\",\"bids\":[" + levels + "],\"asks\":[" + levels + "]}");

        ArgumentCaptor<MarketSnapshot> snapshots = ArgumentCaptor.forClass(MarketSnapshot.class);
        verify(socket).broadcastSnapshot(snapshots.capture());
        assertThat(snapshots.getValue().bids()).hasSize(400);
        assertThat(snapshots.getValue().asks()).hasSize(400);
    }

    private String atasMbo(
        String stream,
        long sequence,
        String action,
        String side,
        String price,
        long volume,
        long orderId
    ) {
        return """
            {"source":"atas","source_stream_id":"%s","source_sequence":%d,
             "event_time":"2026-08-09T09:30:00","type":"%s","side":"%s",
             "price":%s,"volume":%d,"exchange_order_id":%d,"canonical_id":42004177,
             "contract_symbol":"NQU6","exchange":"CME","subscription_id":"atas-nq-u6"}
            """.formatted(stream, sequence, action, side, price, volume, orderId);
    }
}
