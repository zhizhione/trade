package com.realtime.marketdata.orderbook.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.marketdata.mbo.model.LiveMboEvent;
import com.realtime.marketdata.mbo.model.MboEvent;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Compares the historical DBN model with the live-normalized model on a raw JSON export. */
class MboJsonConsistencyTest {
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void historicalAndLiveBookOutputsMatchForJsonExport() throws Exception {
        String configuredPath = System.getProperty("mbo.json", System.getenv("MBO_JSON"));
        Assumptions.assumeTrue(configuredPath != null && !configuredPath.isBlank(),
            "Set -Dmbo.json=/path/to/export.json to run the JSON consistency check");

        JsonNode root = json.readTree(Path.of(configuredPath));
        JsonNode records = root.isArray() ? root : root.path("records");
        Assumptions.assumeTrue(records.isArray(), "JSON must contain a records array");

        MboBookEngineFactory factory = new MboBookEngineFactory();
        MboBookEngine historical = factory.createHistorical();
        MboBookEngine live = factory.createLive();
        List<String> mismatches = new ArrayList<>();
        int crossed = 0;
        int applied = 0;
        int skippedBootstrap = 0;
        String firstSkipped = null;

        for (JsonNode record : records) {
            MboEvent historicalEvent = historicalEvent(record);
            LiveMboEvent liveEvent = liveEvent(record);
            MboBookEngine.BookSnapshot historicalSnapshot;
            MboBookEngine.BookSnapshot liveSnapshot;
            RuntimeException historicalFailure = null;
            RuntimeException liveFailure = null;
            try {
                historicalSnapshot = historical.apply(historicalEvent).orElseThrow();
            } catch (RuntimeException failure) {
                historicalSnapshot = null;
                historicalFailure = failure;
            }
            try {
                liveSnapshot = live.apply(liveEvent).orElseThrow();
            } catch (RuntimeException failure) {
                liveSnapshot = null;
                liveFailure = failure;
            }
            if (historicalFailure != null || liveFailure != null) {
                assertThat(historicalFailure == null)
                    .as("historical/live acceptance differs at sourceOrdinal=%s",
                        Long.toUnsignedString(historicalEvent.sourceOrdinal()))
                    .isEqualTo(liveFailure == null);
                skippedBootstrap += 1;
                if (firstSkipped == null) {
                    firstSkipped = Long.toUnsignedString(historicalEvent.sourceOrdinal())
                        + " (" + historicalFailure.getMessage() + ")";
                }
                continue;
            }
            applied += 1;
            if (historicalSnapshot.crossed()) crossed += 1;

            if (!sameBook(historicalSnapshot, liveSnapshot, historical, live)) {
                if (mismatches.size() < 10) {
                    mismatches.add("sourceOrdinal=" + Long.toUnsignedString(historicalEvent.sourceOrdinal())
                        + " action=" + historicalEvent.action()
                        + " historical=" + levels(historicalSnapshot)
                        + " live=" + levels(liveSnapshot));
                }
            }
        }

        System.out.printf("JSON consistency: records=%d applied=%d skippedBootstrap=%d crossed=%d mismatches=%d firstSkipped=%s%n",
            records.size(), applied, skippedBootstrap, crossed, mismatches.size(), firstSkipped);
        assertThat(mismatches)
            .as("historical/live mismatches; applied=%s crossed=%s", applied, crossed)
            .isEmpty();
    }

    private MboEvent historicalEvent(JsonNode record) {
        return new MboEvent(
            required(record, "source_ordinal"), required(record, "ts_recv"), required(record, "ts_event"),
            record.path("rtype").intValue(), record.path("publisher_id").intValue(),
            required(record, "instrument_id"), charValue(record, "action"), charValue(record, "side"),
            required(record, "price"), required(record, "size"), record.path("channel_id").intValue(),
            required(record, "order_id"), record.path("flags").intValue(), record.path("ts_in_delta").intValue(),
            required(record, "sequence")
        );
    }

    private LiveMboEvent liveEvent(JsonNode record) {
        return new LiveMboEvent(
            required(record, "source_ordinal"), required(record, "ts_recv"), required(record, "ts_event"),
            record.path("publisher_id").intValue(), required(record, "instrument_id"),
            liveAction(charValue(record, "action")), charValue(record, "side"), required(record, "price"),
            required(record, "size"), required(record, "order_id"), required(record, "sequence")
        );
    }

    private static LiveMboEvent.Action liveAction(char action) {
        return switch (action) {
            case 'A' -> LiveMboEvent.Action.ADD;
            case 'M' -> LiveMboEvent.Action.MODIFY;
            case 'C' -> LiveMboEvent.Action.CANCEL;
            case 'R' -> LiveMboEvent.Action.CLEAR;
            case 'T' -> LiveMboEvent.Action.TRADE;
            case 'F' -> LiveMboEvent.Action.FILL;
            case 'N' -> LiveMboEvent.Action.NOOP;
            default -> throw new IllegalArgumentException("unsupported action=" + action);
        };
    }

    private static boolean sameBook(MboBookEngine.BookSnapshot historical,
                                    MboBookEngine.BookSnapshot live,
                                    MboBookEngine historicalEngine,
                                    MboBookEngine liveEngine) {
        if (!historical.key().equals(live.key())
            || !historical.bids().equals(live.bids())
            || !historical.asks().equals(live.asks())
            || historical.crossed() != live.crossed()) {
            return false;
        }
        for (MboBookEngine.Level level : historical.bids()) {
            if (!historicalEngine.ordersAtLevel(historical.key().publisherId(), historical.key().instrumentId(), 'B', level.priceNano())
                .equals(liveEngine.ordersAtLevel(live.key().publisherId(), live.key().instrumentId(), 'B', level.priceNano()))) {
                return false;
            }
        }
        for (MboBookEngine.Level level : historical.asks()) {
            if (!historicalEngine.ordersAtLevel(historical.key().publisherId(), historical.key().instrumentId(), 'A', level.priceNano())
                .equals(liveEngine.ordersAtLevel(live.key().publisherId(), live.key().instrumentId(), 'A', level.priceNano()))) {
                return false;
            }
        }
        return true;
    }

    private static String levels(MboBookEngine.BookSnapshot snapshot) {
        return snapshot.bids() + "/" + snapshot.asks() + " crossed=" + snapshot.crossed();
    }

    private static long required(JsonNode record, String field) {
        JsonNode value = record.path(field);
        if (!value.isNumber()) throw new IllegalArgumentException("missing numeric field=" + field);
        return value.longValue();
    }

    private static char charValue(JsonNode record, String field) {
        String value = record.path(field).asString();
        if (value.length() != 1) throw new IllegalArgumentException("missing char field=" + field);
        return value.charAt(0);
    }
}
