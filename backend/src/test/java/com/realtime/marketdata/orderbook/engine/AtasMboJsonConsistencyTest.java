package com.realtime.marketdata.orderbook.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.marketdata.mbo.model.LiveMboEvent;
import com.realtime.marketdata.mbo.model.MboEvent;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Compares ATAS history-normalization and live-normalization on an exported MBO JSON stream. */
class AtasMboJsonConsistencyTest {
    private static final int PUBLISHER_ID = 1;
    private static final long INSTRUMENT_ID = 42_004_177L;
    private static final int RTYPE_MBO = 160;
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void historicalAndLiveBookOutputsMatchForAtasJsonExport() throws Exception {
        String configuredPath = System.getProperty("atas.json", System.getenv("ATAS_JSON"));
        Assumptions.assumeTrue(configuredPath != null && !configuredPath.isBlank(),
            "Set ATAS_JSON=/path/to/atas-mbo.json to run the ATAS consistency check");

        JsonNode root = json.readTree(Path.of(configuredPath));
        Assumptions.assumeTrue(root.isArray(), "ATAS JSON must be an array");

        MboBookEngineFactory factory = new MboBookEngineFactory();
        MboBookEngine historical = factory.createHistorical();
        MboBookEngine live = factory.createLive();
        List<String> mismatches = new ArrayList<>();
        int records = 0;
        int applied = 0;
        int skippedBootstrap = 0;
        int crossed = 0;
        int priorityMetadataMismatchEvents = 0;
        String firstSkipped = null;

        for (JsonNode record : root) {
            if (!"mbo".equals(record.path("record_type").asString())) continue;
            records += 1;
            MboEvent historicalEvent = historicalEvent(record);
            LiveMboEvent liveEvent = liveEvent(record);
            MboBookEngine.BookSnapshot historicalSnapshot = null;
            MboBookEngine.BookSnapshot liveSnapshot = null;
            RuntimeException historicalFailure = null;
            RuntimeException liveFailure = null;
            try {
                historicalSnapshot = historical.apply(historicalEvent).orElseThrow();
            } catch (RuntimeException failure) {
                historicalFailure = failure;
            }
            try {
                liveSnapshot = live.apply(liveEvent).orElseThrow();
            } catch (RuntimeException failure) {
                liveFailure = failure;
            }
            if (historicalFailure != null || liveFailure != null) {
                assertThat(historicalFailure == null)
                    .as("historical/live acceptance differs at ATAS sequence=%s", historicalEvent.sequence())
                    .isEqualTo(liveFailure == null);
                skippedBootstrap += 1;
                if (firstSkipped == null) {
                    firstSkipped = historicalEvent.sequence() + " (" + historicalFailure.getMessage() + ")";
                }
                continue;
            }

            applied += 1;
            if (historicalSnapshot.crossed()) crossed += 1;
            BookComparison comparison = compareBook(historicalSnapshot, liveSnapshot, historical, live);
            if (!comparison.priorityEqual()) priorityMetadataMismatchEvents += 1;
            if (!comparison.outputEqual()) {
                if (mismatches.size() < 10) {
                    mismatches.add("sequence=" + historicalEvent.sequence()
                        + " updateType=" + record.path("update_type").asString()
                        + " historical=" + levels(historicalSnapshot)
                        + " live=" + levels(liveSnapshot));
                }
            }
        }

        System.out.printf("ATAS consistency: records=%d applied=%d skippedBootstrap=%d crossed=%d outputMismatches=%d priorityMetadataMismatchEvents=%d firstSkipped=%s%n",
            records, applied, skippedBootstrap, crossed, mismatches.size(), priorityMetadataMismatchEvents, firstSkipped);
        assertThat(mismatches)
            .as("ATAS historical/live mismatches; records=%s applied=%s skippedBootstrap=%s",
                records, applied, skippedBootstrap)
            .isEmpty();
    }

    private MboEvent historicalEvent(JsonNode record) {
        char action = action(record.path("update_type").asString());
        return new MboEvent(
            sequence(record), receivedNs(record), eventNs(record), RTYPE_MBO, PUBLISHER_ID, INSTRUMENT_ID,
            action, side(record), priceNano(record), record.path("volume").longValue(), 0,
            record.path("exchange_order_id").longValue(), 0, 0, sequence(record)
        );
    }

    private LiveMboEvent liveEvent(JsonNode record) {
        return new LiveMboEvent(
            sequence(record), receivedNs(record), eventNs(record), PUBLISHER_ID, INSTRUMENT_ID,
            liveAction(record.path("update_type").asString()), side(record), priceNano(record),
            record.path("volume").longValue(), record.path("exchange_order_id").longValue(), sequence(record),
            record.has("priority") ? record.path("priority").longValue() : null
        );
    }

    private static char action(String updateType) {
        return switch (updateType) {
            case "New" -> 'A';
            case "Change" -> 'M';
            case "Delete" -> 'C';
            default -> throw new IllegalArgumentException("unsupported ATAS update_type=" + updateType);
        };
    }

    private static LiveMboEvent.Action liveAction(String updateType) {
        return switch (updateType) {
            case "New" -> LiveMboEvent.Action.ADD;
            case "Change" -> LiveMboEvent.Action.MODIFY;
            case "Delete" -> LiveMboEvent.Action.DELETE;
            default -> throw new IllegalArgumentException("unsupported ATAS update_type=" + updateType);
        };
    }

    private static char side(JsonNode record) {
        return switch (record.path("side").asString()) {
            case "Bid" -> 'B';
            case "Ask" -> 'A';
            default -> throw new IllegalArgumentException("unsupported ATAS side=" + record.path("side").asString());
        };
    }

    private static long sequence(JsonNode record) {
        return record.path("sequence").longValue();
    }

    private static long receivedNs(JsonNode record) {
        return instantNs(Instant.parse(record.path("received_utc").asString()));
    }

    private static long eventNs(JsonNode record) {
        return instantNs(Instant.parse(record.path("event_time").asString() + "Z"));
    }

    private static long instantNs(Instant value) {
        return Math.addExact(Math.multiplyExact(value.getEpochSecond(), 1_000_000_000L), value.getNano());
    }

    private static long priceNano(JsonNode record) {
        return record.path("price").decimalValue()
            .movePointRight(9).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
    }

    private static BookComparison compareBook(MboBookEngine.BookSnapshot historical,
                                              MboBookEngine.BookSnapshot live,
                                              MboBookEngine historicalEngine,
                                              MboBookEngine liveEngine) {
        boolean priorityEqual = true;
        if (!historical.key().equals(live.key())
            || !historical.bids().equals(live.bids())
            || !historical.asks().equals(live.asks())
            || historical.crossed() != live.crossed()) {
            return new BookComparison(false, priorityEqual);
        }
        for (MboBookEngine.Level level : historical.bids()) {
            QueueComparison queue = compareQueue(
                historicalEngine.ordersAtLevel(PUBLISHER_ID, INSTRUMENT_ID, 'B', level.priceNano()),
                liveEngine.ordersAtLevel(PUBLISHER_ID, INSTRUMENT_ID, 'B', level.priceNano())
            );
            if (!queue.outputEqual()) return new BookComparison(false, queue.priorityEqual());
            priorityEqual &= queue.priorityEqual();
        }
        for (MboBookEngine.Level level : historical.asks()) {
            QueueComparison queue = compareQueue(
                historicalEngine.ordersAtLevel(PUBLISHER_ID, INSTRUMENT_ID, 'A', level.priceNano()),
                liveEngine.ordersAtLevel(PUBLISHER_ID, INSTRUMENT_ID, 'A', level.priceNano())
            );
            if (!queue.outputEqual()) return new BookComparison(false, queue.priorityEqual());
            priorityEqual &= queue.priorityEqual();
        }
        return new BookComparison(true, priorityEqual);
    }

    private static QueueComparison compareQueue(List<MboBookEngine.OrderView> historical,
                                                List<MboBookEngine.OrderView> live) {
        if (historical.size() != live.size()) return new QueueComparison(false, false);
        boolean priorityEqual = true;
        for (int index = 0; index < historical.size(); index += 1) {
            MboBookEngine.OrderView left = historical.get(index);
            MboBookEngine.OrderView right = live.get(index);
            if (left.orderId() != right.orderId() || left.side() != right.side()
                || left.priceNano() != right.priceNano() || left.size() != right.size()) {
                return new QueueComparison(false, false);
            }
            priorityEqual &= left.priorityOrdinal() == right.priorityOrdinal();
        }
        return new QueueComparison(true, priorityEqual);
    }

    private static String levels(MboBookEngine.BookSnapshot snapshot) {
        return snapshot.bids() + "/" + snapshot.asks() + " crossed=" + snapshot.crossed();
    }

    private record BookComparison(boolean outputEqual, boolean priorityEqual) {
    }

    private record QueueComparison(boolean outputEqual, boolean priorityEqual) {
    }
}
