package com.realtime.marketdata.market.processor;

import com.realtime.marketdata.core.event.MarketEvent;
import com.realtime.marketdata.marketstate.model.MarketSnapshot;
import com.realtime.marketdata.marketstate.model.MarketSnapshot.DepthLevel;
import com.realtime.marketdata.market.port.MarketEventStore;
import com.realtime.marketdata.market.port.MarketUpdatePublisher;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * 行情领域入口：解析来源 JSON、更新内存展示快照、持久化并推送前端。
 * 原始表持久化与 WebSocket 展示是不同职责；快照只描述当前可视状态，不能作为盘口
 * 重建或历史审计的输入。
 */
@Service
public class MarketEventService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final ObjectMapper objectMapper;
    private final MarketEventStore storageRepository;
    private final MarketUpdatePublisher updatePublisher;
    private final RealtimeMboBookService realtimeMboBookService;
    private final ZoneId atasEventTimeZone;
    private final ConcurrentMap<String, SnapshotAccumulator> snapshots = new ConcurrentHashMap<>();

    public MarketEventService(
        ObjectMapper objectMapper,
        MarketEventStore storageRepository,
        MarketUpdatePublisher updatePublisher,
        RealtimeMboBookService realtimeMboBookService,
        @org.springframework.beans.factory.annotation.Value("${app.atas.event-time-zone:America/Chicago}")
        String atasEventTimeZone
    ) {
        this.objectMapper = objectMapper;
        this.storageRepository = storageRepository;
        this.updatePublisher = updatePublisher;
        this.realtimeMboBookService = realtimeMboBookService;
        this.atasEventTimeZone = ZoneId.of(atasEventTimeZone);
    }

    /**
     * 处理一条来自 Kafka 或其他接入层的 JSON 消息。执行顺序固定为“解析 → 更新内存快照
     * → 持久化 → 对外发布”。来源字段保持原样；ATAS 若未提供 canonical_id，涉及该字段的
     * 持久化会跳过该事件，但展示与广播不受影响。
     */
    public MarketEvent process(String topic, String rawMessage) throws JacksonException {
        JsonNode source = objectMapper.readTree(rawMessage);
        if (source == null || !source.isObject()) {
            throw new IllegalArgumentException("Market event must be a JSON object");
        }

        MarketEvent event = normalize(topic, source);
        MarketSnapshot snapshot = updateSnapshot(event);

        storageRepository.save(event);
        updatePublisher.broadcastEvent(event);
        updatePublisher.broadcastSnapshot(snapshot);
        return event;
    }

    private MarketEvent normalize(String topic, JsonNode source) {
        Instant receivedAt = Instant.now();
        String eventType = firstText(
            source,
            "eventType",
            "event_type",
            "dataType",
            "data_type",
            "messageType",
            "message_type"
        );
        String candidateType = firstText(source, "type");
        if (eventType == null && candidateType != null
            && !(isAtasEvent(source) && isAtasMboAction(candidateType))) {
            eventType = candidateType;
        }
        if (eventType == null) {
            int separator = topic.lastIndexOf('.');
            eventType = separator >= 0 ? topic.substring(separator + 1) : topic;
        }

        String sourceName = sourceOf(source);
        ObjectNode enrichedData = (ObjectNode) source.deepCopy();
        Long canonicalId = parseLong(firstNode(source, "canonical_id", "canonicalId"));
        // Databento 的 instrument_id 是来源内稳定的数值身份；不再依赖外部映射表重写它。
        if (canonicalId == null && "databento".equals(sourceName)) {
            canonicalId = parseLong(firstNode(source, "instrument_id", "instrumentId"));
        }
        String symbol = firstText(source, "contractSymbol", "contract_symbol", "symbol", "instrument", "ticker");
        if (symbol == null) {
            symbol = canonicalId == null ? "UNKNOWN" : "instrument-" + canonicalId;
        }

        String eventId = firstText(source, "eventId", "event_id", "id");
        return new MarketEvent(
            eventId == null ? UUID.randomUUID().toString() : eventId,
            topic,
            eventType.toLowerCase(Locale.ROOT),
            sourceName,
            canonicalId,
            symbol.toUpperCase(Locale.ROOT),
            parseInstant(
                firstNode(source, "ts_event", "eventTime", "event_time", "timestamp", "ts"),
                receivedAt,
                isAtasEvent(source) ? atasEventTimeZone : ZoneId.of("UTC")
            ),
            receivedAt,
            parseLong(firstNode(source, "sequence", "seq", "sourceSequence", "source_sequence")),
            parseDecimal(firstNode(source, "price", "lastPrice", "last_price")),
            parseDecimal(firstNode(source, "quantity", "qty", "size", "volume")),
            upper(firstText(source, "side", "direction", "aggressorSide", "aggressor_side")),
            enrichedData
        );
    }

    private MarketSnapshot updateSnapshot(MarketEvent event) {
        // 同一合约的不同实时连接可能各自从 sequence=0 开始；必须分开维护状态。
        SnapshotAccumulator state = snapshots.computeIfAbsent(snapshotKey(event), ignored -> new SnapshotAccumulator());
        synchronized (state) {
            state.eventTime = event.eventTime();
            state.lastEventType = event.eventType();
            // MBO 中的价格表示挂单价格，并不必然是实际成交价格；只有非 MBO 事件才可直接更新最新成交价。
            if (event.price() != null && !"mbo".equals(event.eventType())) {
                state.lastPrice = event.price();
            }

            JsonNode data = event.data();
            Optional<MboBookEngine.BookSnapshot> rebuiltBook = realtimeMboBookService.apply(event);
            if (rebuiltBook.isPresent()) {
                updateDepth(state, rebuiltBook.get());
            } else {
                updateDepth(state, data);
            }

            BigDecimal explicitOrderFlow = parseDecimal(firstNode(data, "orderFlow", "order_flow", "imbalance"));
            if (explicitOrderFlow != null) {
                state.orderFlow = explicitOrderFlow;
            } else {
                // 来源未提供累计订单流时，只把成交方向转换成增量；挂单 Add/Cancel 不计入。
                BigDecimal signedQuantity = signedTradeQuantity(event);
                if (signedQuantity != null) {
                    state.orderFlow = state.orderFlow.add(signedQuantity);
                }
            }

            BigDecimal signal = parseDecimal(firstNode(data, "signalValue", "signal_value", "signal", "value"));
            if (signal != null && "signal".equals(event.eventType())) {
                state.signalValue = signal;
            }

            return new MarketSnapshot(
                event.source(),
                event.canonicalId(),
                sourceStreamId(event),
                event.symbol(),
                state.eventTime,
                state.lastPrice,
                List.copyOf(state.bids),
                List.copyOf(state.asks),
                state.orderFlow,
                state.signalValue,
                state.lastEventType
            );
        }
    }

    private void updateDepth(SnapshotAccumulator state, MboBookEngine.BookSnapshot book) {
        state.bids = depthLevels(book.bids());
        state.asks = depthLevels(book.asks());
    }

    private List<DepthLevel> depthLevels(List<MboBookEngine.Level> levels) {
        return levels.stream()
            .limit(MboBookEngine.MAX_DEPTH)
            .map(level -> new DepthLevel(
                BigDecimal.valueOf(level.priceNano(), 9),
                BigDecimal.valueOf(level.size())
            ))
            .toList();
    }

    private void updateDepth(SnapshotAccumulator state, JsonNode data) {
        JsonNode bidLevels = firstNode(data, "bids", "bidLevels", "bid_levels");
        JsonNode askLevels = firstNode(data, "asks", "askLevels", "ask_levels");
        if (bidLevels != null && bidLevels.isArray()) {
            state.bids = parseDepth(bidLevels);
        }
        if (askLevels != null && askLevels.isArray()) {
            state.asks = parseDepth(askLevels);
        }

        BigDecimal bidPrice = parseDecimal(firstNode(data, "bidPrice", "bid_price", "bestBid", "best_bid"));
        BigDecimal bidQuantity = parseDecimal(firstNode(data, "bidQuantity", "bid_quantity", "bidSize", "bid_size"));
        // 最优买卖价字段是单档快照；出现时优先覆盖同侧的多档状态，不能与旧档位混合。
        if (bidPrice != null) {
            state.bids = List.of(new DepthLevel(bidPrice, bidQuantity == null ? ZERO : bidQuantity));
        }

        BigDecimal askPrice = parseDecimal(firstNode(data, "askPrice", "ask_price", "bestAsk", "best_ask"));
        BigDecimal askQuantity = parseDecimal(firstNode(data, "askQuantity", "ask_quantity", "askSize", "ask_size"));
        if (askPrice != null) {
            state.asks = List.of(new DepthLevel(askPrice, askQuantity == null ? ZERO : askQuantity));
        }
    }

    private List<DepthLevel> parseDepth(JsonNode levels) {
        List<DepthLevel> result = new ArrayList<>();
        for (JsonNode level : levels) {
            BigDecimal price;
            BigDecimal quantity;
            if (level.isArray() && level.size() >= 2) {
                price = parseDecimal(level.get(0));
                quantity = parseDecimal(level.get(1));
            } else {
                price = parseDecimal(firstNode(level, "price", "px"));
                quantity = parseDecimal(firstNode(level, "quantity", "qty", "size"));
            }
            if (price != null && quantity != null) {
                result.add(new DepthLevel(price, quantity));
                if (result.size() == MboBookEngine.MAX_DEPTH) {
                    break;
                }
            }
        }
        return result;
    }

    private JsonNode firstNode(JsonNode source, String... fields) {
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

    private String firstText(JsonNode source, String... fields) {
        JsonNode value = firstNode(source, fields);
        if (value == null) {
            return null;
        }
        String text = value.asString().trim();
        return text.isEmpty() ? null : text;
    }

    private BigDecimal parseDecimal(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        try {
            return value.isNumber() ? value.decimalValue() : new BigDecimal(value.asString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Long parseLong(JsonNode value) {
        if (value == null || value.isNull()) {
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

    private boolean isTradeForOrderFlow(MarketEvent event) {
        if ("trade".equals(event.eventType())) {
            return true;
        }
        if (!"mbo".equals(event.eventType())) {
            return false;
        }
        String action = firstText(event.data(), "action", "updateType", "update_type");
        return action != null && ("trade".equalsIgnoreCase(action) || "fill".equalsIgnoreCase(action));
    }

    private BigDecimal signedTradeQuantity(MarketEvent event) {
        if (event.quantity() == null || !isTradeForOrderFlow(event)) {
            return null;
        }
        String direction = upper(firstText(event.data(), "direction", "aggressorSide", "aggressor_side"));
        if (direction == null && "trade".equals(event.eventType())) {
            direction = event.side();
        }
        if (direction == null && "mbo".equals(event.eventType())) {
            // MBO 的 side 是被动挂单侧：Ask 被成交意味着主动买入，Bid 则意味着主动卖出。
            String passiveSide = upper(firstText(event.data(), "side"));
            direction = switch (passiveSide == null ? "" : passiveSide) {
                case "ASK", "A" -> "BUY";
                case "BID", "B" -> "SELL";
                default -> null;
            };
        }
        return switch (direction == null ? "" : direction) {
            case "BUY", "B" -> event.quantity();
            case "SELL", "S" -> event.quantity().negate();
            default -> null;
        };
    }

    private boolean isAtasMboAction(String value) {
        return "new".equalsIgnoreCase(value)
            || "change".equalsIgnoreCase(value)
            || "delete".equalsIgnoreCase(value);
    }

    private boolean isAtasEvent(JsonNode source) {
        String feed = firstText(source, "source", "feed", "provider", "vendor");
        return (feed != null && feed.toLowerCase(Locale.ROOT).contains("atas"))
            || firstNode(source, "sourceStreamId", "source_stream_id", "streamId", "stream_id") != null
            || firstNode(source, "eventTimeKind", "event_time_kind") != null;
    }

    private String sourceOf(JsonNode source) {
        String feed = firstText(source, "source", "feed", "provider", "vendor");
        if (feed != null) {
            String normalized = feed.toLowerCase(Locale.ROOT);
            if (normalized.contains("atas")) {
                return "atas";
            }
            if (normalized.contains("databento")) {
                return "databento";
            }
        }
        if (firstNode(source, "dataset", "publisher_id", "publisherId", "instrument_id", "instrumentId") != null) {
            return "databento";
        }
        if (firstNode(source, "source_stream_id", "sourceStreamId", "stream_id", "streamId") != null) {
            return "atas";
        }
        if (firstNode(source, "event_time_kind", "eventTimeKind") != null) {
            return "atas";
        }
        return null;
    }

    private String snapshotKey(MarketEvent event) {
        if (event.canonicalId() == null) {
            return "generic:" + event.symbol();
        }
        String sourceStreamId = sourceStreamId(event);
        return "%s:%d:%s".formatted(
            event.source(),
            event.canonicalId(),
            sourceStreamId == null ? "" : sourceStreamId
        );
    }

    private String sourceStreamId(MarketEvent event) {
        return firstText(event.data(), "source_stream_id", "sourceStreamId", "stream_id", "streamId");
    }

    private Instant parseInstant(JsonNode value, Instant fallback, ZoneId localTimeZone) {
        if (value == null || value.isNull()) {
            return fallback;
        }
        if (value.isNumber()) {
            // 接入层可能收到原始 Databento ts_event（纳秒）、毫秒或秒。
            // 先按数量级识别单位，避免把约 1.7e18 纳秒误当成毫秒。
            long epoch = value.longValue();
            long magnitude = epoch == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(epoch);
            if (magnitude >= 1_000_000_000_000_000L) {
                long seconds = Math.floorDiv(epoch, 1_000_000_000L);
                long nanos = Math.floorMod(epoch, 1_000_000_000L);
                return Instant.ofEpochSecond(seconds, nanos);
            }
            return magnitude < 10_000_000_000L
                ? Instant.ofEpochSecond(epoch)
                : Instant.ofEpochMilli(epoch);
        }
        try {
            return Instant.parse(value.asString());
        } catch (DateTimeParseException firstFailure) {
            try {
                return OffsetDateTime.parse(value.asString()).toInstant();
            } catch (DateTimeParseException secondFailure) {
                try {
                    return LocalDateTime.parse(value.asString().replace(' ', 'T')).atZone(localTimeZone).toInstant();
                } catch (DateTimeParseException ignored) {
                    throw new IllegalArgumentException("Invalid market event timestamp: " + value.asString());
                }
            }
        }
    }

    private String upper(String value) {
        return value == null ? null : value.toUpperCase(Locale.ROOT);
    }

    private static final class SnapshotAccumulator {
        private Instant eventTime;
        private BigDecimal lastPrice;
        private List<DepthLevel> bids = List.of();
        private List<DepthLevel> asks = List.of();
        private BigDecimal orderFlow = ZERO;
        private BigDecimal signalValue = ZERO;
        private String lastEventType;
    }
}
