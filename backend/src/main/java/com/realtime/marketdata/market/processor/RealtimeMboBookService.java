package com.realtime.marketdata.market.processor;

import com.realtime.marketdata.core.event.MarketEvent;
import com.realtime.marketdata.mbo.model.LiveMboEvent;
import com.realtime.marketdata.mbo.processor.MboStreamKey;
import com.realtime.marketdata.mbo.processor.MboStreamProcessor;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import com.realtime.marketdata.orderbook.engine.MboBookInvariantException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将有序 ATAS MBO 消息转换为与传输方式无关的 Java L3 状态机输入。
 *
 * <p>实时订单簿只有在来源同时提供稳定流身份、单调递增的来源序号、已解析的合约身份和逐订单
 * 字段时才能可靠重建；不完整消息不进入状态机，但由持久化层写入 rejected raw 表供后续排查。</p>
 */
@Service
public final class RealtimeMboBookService {
    private static final int ATAS_PUBLISHER_ID = 0;
    private static final Logger log = LoggerFactory.getLogger(RealtimeMboBookService.class);
    private final MboStreamProcessor streams = new MboStreamProcessor(new MboBookEngineFactory());
    private final Set<MboStreamKey> desynchronizedStreams = ConcurrentHashMap.newKeySet();

    /** Explicitly drops a source stream after disconnect or provider session rollover. */
    public void closeStream(String streamId) {
        if (streamId != null && !streamId.isBlank()) {
            MboStreamKey key = new MboStreamKey("atas", streamId);
            desynchronizedStreams.remove(key);
            streams.close(key);
        }
    }

    /** Returns whether incremental updates for a stream are quarantined after an invariant error. */
    public boolean isDesynchronized(String streamId) {
        return streamId != null && !streamId.isBlank()
            && desynchronizedStreams.contains(new MboStreamKey("atas", streamId));
    }

    @Scheduled(fixedDelayString = "${app.atas.mbo-stream-eviction-ms:300000}")
    void evictIdleStreams() {
        streams.evictIdle(Duration.ofMinutes(30));
    }

    /**
     * 应用一条完整 ATAS MBO 消息并返回重建后的盘口深度。非 MBO 消息、不支持的来源或字段
     * 不完整的消息不会触碰状态机，避免错误输入破坏已有订单簿。
     */
    /** Backwards-compatible snapshot-only API; desynchronized and ignored inputs return empty. */
    public Optional<MboBookEngine.BookSnapshot> apply(MarketEvent event) {
        return applyDetailed(event).optionalSnapshot();
    }

    /** Applies an event while preserving whether an empty result means ignored or desynchronized. */
    public ApplyResult applyDetailed(MarketEvent event) {
        Optional<NormalizedLiveUpdate> update = normalize(event);
        if (update.isEmpty()) {
            return ApplyResult.ignored();
        }
        NormalizedLiveUpdate normalized = update.get();
        if (desynchronizedStreams.contains(normalized.stream())) {
            return ApplyResult.desynchronized(
                normalized.stream().streamId(), "stream is quarantined; await explicit close/reset"
            );
        }
        try {
            return ApplyResult.applied(
                streams.accept(normalized.stream(), normalized.event()).orElseThrow(),
                normalized.stream().streamId()
            );
        } catch (MboBookInvariantException exception) {
            // A bad update makes the incremental book untrustworthy.  Drop only this source
            // stream so a later provider snapshot/reconnect can rebuild it from a clean state;
            // the raw event is still persisted by the caller.
            streams.close(normalized.stream());
            desynchronizedStreams.add(normalized.stream());
            log.warn(
                "ATAS MBO stream desynchronized; waiting for a fresh stream: streamId={}, sequence={}, reason={}",
                normalized.stream().streamId(), normalized.event().sourceOrdinal(), exception.getMessage()
            );
            return ApplyResult.desynchronized(normalized.stream().streamId(), exception.getMessage());
        }
    }

    public record ApplyResult(Status status, MboBookEngine.BookSnapshot snapshot, String streamId, String reason) {
        public enum Status {
            IGNORED,
            APPLIED,
            DESYNCHRONIZED
        }

        private Optional<MboBookEngine.BookSnapshot> optionalSnapshot() {
            return Optional.ofNullable(snapshot);
        }

        private static ApplyResult ignored() {
            return new ApplyResult(Status.IGNORED, null, null, null);
        }

        private static ApplyResult applied(MboBookEngine.BookSnapshot snapshot, String streamId) {
            return new ApplyResult(Status.APPLIED, snapshot, streamId, null);
        }

        private static ApplyResult desynchronized(String streamId, String reason) {
            return new ApplyResult(Status.DESYNCHRONIZED, null, streamId, reason);
        }
    }

    private Optional<NormalizedLiveUpdate> normalize(MarketEvent event) {
        if (!"mbo".equals(event.eventType()) || !"atas".equals(event.source())) {
            return Optional.empty();
        }

        JsonNode data = event.data();
        String streamId = text(data, "source_stream_id", "sourceStreamId", "stream_id", "streamId");
        String actionText = text(data, "update_type", "updateType", "action", "type");
        LiveMboEvent.Action action = action(actionText);
        Long sourceSequence = event.sequence();
        Long instrumentId = event.canonicalId();
        Long orderId = longValue(data, "exchange_order_id", "exchangeOrderId", "order_id", "orderId");
        Character side = side(event.side());
        if (side == null) {
            side = side(text(data, "side", "order_side", "orderSide"));
        }
        Long priceNano = priceNano(event.price());
        if (priceNano == null) {
            priceNano = priceNano(decimal(data, "price", "px", "order_price", "orderPrice"));
        }

        boolean delete = action == LiveMboEvent.Action.DELETE;
        if (streamId == null || sourceSequence == null || instrumentId == null || action == null
            || orderId == null || (!delete && (side == null || priceNano == null))) {
            return Optional.empty();
        }
        if (side == null) side = 'N';
        if (priceNano == null) priceNano = LiveMboEvent.MISSING_PRICE_NANO;
        Long priority = unsignedLongValue(data, "priority", "queue_priority", "queuePriority");

        long size = size(event.quantity(), action);
        if (size < 0) {
            return Optional.empty();
        }
        LiveMboEvent liveEvent = new LiveMboEvent(
            sourceSequence,
            epochNanos(event.receivedAt()),
            epochNanos(event.eventTime()),
            ATAS_PUBLISHER_ID,
            instrumentId,
            action,
            side,
            priceNano,
            size,
            orderId,
            sourceSequence,
            priority
        );
        return Optional.of(new NormalizedLiveUpdate(
            new MboStreamKey(event.source(), streamId),
            liveEvent
        ));
    }

    private LiveMboEvent.Action action(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "NEW", "ADD", "A" -> LiveMboEvent.Action.ADD;
            case "CHANGE", "MODIFY", "M" -> LiveMboEvent.Action.MODIFY;
            case "DELETE", "REMOVE", "D" -> LiveMboEvent.Action.DELETE;
            default -> null;
        };
    }

    private Character side(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "BID", "B" -> 'B';
            case "ASK", "A" -> 'A';
            default -> null;
        };
    }

    private long size(BigDecimal value, LiveMboEvent.Action action) {
        if (action == LiveMboEvent.Action.DELETE) {
            return 0;
        }
        if (value == null) {
            return -1;
        }
        try {
            long result = value.longValueExact();
            return result > 0 ? result : -1;
        } catch (ArithmeticException ignored) {
            return -1;
        }
    }

    private BigDecimal decimal(JsonNode source, String... fields) {
        JsonNode value = node(source, fields);
        if (value == null) return null;
        try {
            return value.isNumber() ? value.decimalValue() : new BigDecimal(value.asString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Long priceNano(BigDecimal price) {
        if (price == null) {
            return null;
        }
        try {
            return price.movePointRight(9).longValueExact();
        } catch (ArithmeticException ignored) {
            return null;
        }
    }

    private long epochNanos(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
    }

    private String text(JsonNode source, String... fields) {
        JsonNode value = node(source, fields);
        if (value == null) {
            return null;
        }
        String text = value.asString().trim();
        return text.isEmpty() ? null : text;
    }

    private Long longValue(JsonNode source, String... fields) {
        JsonNode value = node(source, fields);
        if (value == null) {
            return null;
        }
        try {
            if (value.isIntegralNumber()) {
                return value.longValue();
            }
            return Long.valueOf(value.asString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Long unsignedLongValue(JsonNode source, String... fields) {
        JsonNode value = node(source, fields);
        if (value == null) {
            return null;
        }
        try {
            if (value.isIntegralNumber()) {
                return value.longValue();
            }
            return Long.parseUnsignedLong(value.asString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private JsonNode node(JsonNode source, String... fields) {
        if (source == null) {
            return null;
        }
        for (String field : fields) {
            JsonNode value = source.get(field);
            if (value != null && !value.isNull()) {
                return value;
            }
        }
        return null;
    }

    private record NormalizedLiveUpdate(MboStreamKey stream, LiveMboEvent event) {
    }
}
