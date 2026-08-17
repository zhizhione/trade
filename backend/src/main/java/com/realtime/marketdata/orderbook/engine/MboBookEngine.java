package com.realtime.marketdata.orderbook.engine;

import com.realtime.marketdata.mbo.model.LiveMboEvent;
import com.realtime.marketdata.mbo.model.MboEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 用于 Databento MBO 事件的确定性 L3 订单簿状态机。
 *
 * <p>引擎有意与传输协议和数据库实现解耦。它既接收历史 {@link MboEvent} 原始记录，也接收
 * 归一化后的 {@link LiveMboEvent} 实时更新；二者执行完全相同的订单簿状态迁移。当顺序号仅在
 * 单个实时连接内有序时，调用方必须按来源流隔离引擎实例。</p>
 */
public final class MboBookEngine {
    /** 表示快照返回当前订单簿的全部价位。 */
    public static final int UNBOUNDED_DEPTH = Integer.MAX_VALUE;
    /** Databento 标志：该记录是当前消息的最后一条事件。 */
    public static final int F_LAST = 1 << 7;
    /** 拒绝已经派生为最优买卖价（TOB）的记录；MBO 引擎只能接收原始逐订单数据。 */
    public static final int F_TOB = 1 << 6;
    /** 拒绝已经派生为按价聚合盘口（MBP）的记录，防止将聚合结果再次写入逐订单状态机。 */
    public static final int F_MBP = 1 << 4;

    private final Map<BookKey, OrderBook> books = new HashMap<>();
    private final boolean rejectCrossedBooks;
    private final int snapshotDepth;
    private boolean hasAppliedEvent;
    private long lastSourceOrdinal;

    /** 创建严格模式引擎：发现交叉盘即失败，适用于生产校验和执行场景。 */
    public MboBookEngine() {
        this(true, UNBOUNDED_DEPTH);
    }

    /**
     * 验证和研究调用方可关闭交叉盘拒绝，以保留带有 {@code crossed} 标记的异常快照，
     * 便于定位数据源或重建规则问题。
     */
    public MboBookEngine(boolean rejectCrossedBooks) {
        this(rejectCrossedBooks, UNBOUNDED_DEPTH);
    }

    public MboBookEngine(boolean rejectCrossedBooks, int snapshotDepth) {
        validateDepth(snapshotDepth);
        this.rejectCrossedBooks = rejectCrossedBooks;
        this.snapshotDepth = snapshotDepth;
    }

    /**
     * 应用一条历史事件。只有到达 {@link #F_LAST} 消息边界时才返回快照，这与 Databento
     * 的消息分帧一致，避免把半条消息的中间状态暴露给下游。
     */
    public Optional<BookSnapshot> apply(MboEvent event) {
        requireStrictlyIncreasingOrdinal(event.sourceOrdinal());
        if ((event.flags() & (F_TOB | F_MBP)) != 0) {
            throw new MboBookInvariantException("MBO engine rejects F_TOB/F_MBP records");
        }

        BookKey key = new BookKey(event.publisherId(), event.instrumentId());
        OrderBook book = books.computeIfAbsent(key, ignored -> new OrderBook());
        book.apply(event);
        hasAppliedEvent = true;
        lastSourceOrdinal = event.sourceOrdinal();

        if ((event.flags() & F_LAST) == 0) {
            return Optional.empty();
        }
        BookSnapshot snapshot = book.snapshot(key, event, snapshotDepth);
        if (rejectCrossedBooks && snapshot.crossed()) {
            throw new MboBookInvariantException(
                "crossed book at sourceOrdinal=" + Long.toUnsignedString(event.sourceOrdinal())
            );
        }
        return Optional.of(snapshot);
    }

    /**
     * 应用一条归一化后的实时 L3 更新。与 DBN 数据不同，每条实时更新均视为完整消息，
     * 因此每次状态更新后都会返回快照。
     */
    public Optional<BookSnapshot> apply(LiveMboEvent event) {
        requireStrictlyIncreasingOrdinal(event.sourceOrdinal());

        BookKey key = new BookKey(event.publisherId(), event.instrumentId());
        OrderBook book = books.computeIfAbsent(key, ignored -> new OrderBook());
        book.apply(event);
        hasAppliedEvent = true;
        lastSourceOrdinal = event.sourceOrdinal();

        BookSnapshot snapshot = book.snapshot(
            key,
            event.sourceOrdinal(),
            event.tsEventNs(),
            event.tsRecvNs(),
            event.sequence(),
            event.action().snapshotAction(),
            event.side(),
            snapshotDepth
        );
        if (rejectCrossedBooks && snapshot.crossed()) {
            throw new MboBookInvariantException(
                "crossed book at sourceOrdinal=" + Long.toUnsignedString(event.sourceOrdinal())
            );
        }
        return Optional.of(snapshot);
    }

    /** 返回指定订单簿的当前状态；若从未接收过该簿的事件，则返回空快照而非空值。 */
    public BookSnapshot snapshot(int publisherId, long instrumentId, int depth) {
        validateDepth(depth);
        BookKey key = new BookKey(publisherId, instrumentId);
        OrderBook book = books.get(key);
        return book == null
            ? new BookSnapshot(key, -1L, 0L, 0L, 0L, 'N', 'N', List.of(), List.of(), false)
            : book.snapshot(key, -1L, 0L, 0L, 0L, 'N', 'N', depth);
    }

    /** 返回某一价位的活跃订单队列，供需要队列位置的回测或诊断逻辑使用。 */
    public List<OrderView> ordersAtLevel(int publisherId, long instrumentId, char side, long priceNano) {
        if (side != 'B' && side != 'A') {
            throw new IllegalArgumentException("side must be B or A");
        }
        OrderBook book = books.get(new BookKey(publisherId, instrumentId));
        return book == null ? List.of() : book.ordersAtLevel(side, priceNano);
    }

    private void requireStrictlyIncreasingOrdinal(long sourceOrdinal) {
        if (hasAppliedEvent && Long.compareUnsigned(sourceOrdinal, lastSourceOrdinal) <= 0) {
            throw new MboBookInvariantException(
                "sourceOrdinal must be strictly increasing: previous="
                    + Long.toUnsignedString(lastSourceOrdinal) + ", current="
                    + Long.toUnsignedString(sourceOrdinal)
            );
        }
    }

    private static void validateDepth(int depth) {
        if (depth < 1) {
            throw new IllegalArgumentException("depth must be positive");
        }
    }

    public record BookKey(int publisherId, long instrumentId) {
    }

    public record Level(long priceNano, long size, int orderCount) {
        public Level {
            if (size <= 0 || orderCount <= 0) {
                throw new IllegalArgumentException("book levels must contain positive size and order count");
            }
        }
    }

    public record OrderView(long orderId, char side, long priceNano, long size, long priorityOrdinal) {
    }

    public record BookSnapshot(
        BookKey key,
        long sourceOrdinal,
        long tsEventNs,
        long tsRecvNs,
        long sequence,
        char action,
        char side,
        List<Level> bids,
        List<Level> asks,
        boolean crossed
    ) {
        public BookSnapshot {
            bids = List.copyOf(bids);
            asks = List.copyOf(asks);
        }

        public boolean hasBbo() {
            return !bids.isEmpty() && !asks.isEmpty();
        }

        public void requireNotCrossed() {
            if (crossed) {
                throw new MboBookInvariantException(
                    "crossed book at sourceOrdinal=" + Long.toUnsignedString(sourceOrdinal)
                );
            }
        }
    }

    private static final class OrderBook {
        private final Map<Long, RestingOrder> orders = new HashMap<>();
        private final NavigableMap<Long, PriceLevel> bids = new TreeMap<>();
        private final NavigableMap<Long, PriceLevel> asks = new TreeMap<>();

        private void apply(MboEvent event) {
            switch (event.action()) {
                case 'A' -> add(event);
                case 'M' -> modify(event);
                case 'C' -> cancel(event);
                case 'R' -> clear();
                // 成交、成交填充和无操作记录携带成交或分帧信息，但不会直接变更挂单表。
                // 若某交易所明确规定 fill 应扣减剩余量，可在后续接入层加入对应策略。
                case 'T', 'F', 'N' -> { }
                default -> throw new MboBookInvariantException("unsupported MBO action=" + event.action());
            }
        }

        private void apply(LiveMboEvent event) {
            switch (event.action()) {
                case ADD -> add(asHistoricalEvent(event, 'A'));
                case MODIFY -> modify(asHistoricalEvent(event, 'M'));
                case CANCEL -> cancel(asHistoricalEvent(event, 'C'));
                case DELETE -> delete(event);
                case CLEAR -> clear();
                case TRADE, FILL, NOOP -> { }
            }
        }

        private MboEvent asHistoricalEvent(LiveMboEvent event, char action) {
            // 实时适配器完成来源语义归一化后，内部价位迁移与历史事件完全一致。
            // 此处 rtype/flags 仅用于构造兼容事件，不会作为实时原始数据持久化。
            return new MboEvent(
                event.sourceOrdinal(), event.tsRecvNs(), event.tsEventNs(), 160,
                event.publisherId(), event.instrumentId(), action, event.side(),
                event.priceNano(), event.size(), 0, event.orderId(), F_LAST, 0, event.sequence()
            );
        }

        private void add(MboEvent event) {
            requireBookSide(event);
            if (event.size() == 0) {
                throw new MboBookInvariantException("zero-size Add");
            }
            if (orders.containsKey(event.orderId())) {
                throw new MboBookInvariantException(
                    "duplicate active order_id=" + Long.toUnsignedString(event.orderId())
                );
            }
            RestingOrder order = new RestingOrder(
                event.orderId(), event.side(), event.priceNano(), event.size(), event.sourceOrdinal()
            );
            orders.put(order.orderId, order);
            level(order.side, order.priceNano, true).orders.put(order.orderId, order);
        }

        private void modify(MboEvent event) {
            requireBookSide(event);
            if (event.size() == 0) {
                throw new MboBookInvariantException("zero-size Modify");
            }
            RestingOrder old = requireOrder(event.orderId(), "Modify");
            if (old.side != event.side()) {
                throw new MboBookInvariantException("Modify changed side");
            }

            // CME 风格队列规则：改价或增量会失去原有时间优先级；同价减量保留原队列位置。
            boolean losesPriority = old.priceNano != event.priceNano() || old.size < event.size();
            PriceLevel oldLevel = level(old.side, old.priceNano, false);
            oldLevel.orders.remove(old.orderId);
            removeLevelIfEmpty(old.side, old.priceNano, oldLevel);

            long priorityOrdinal = losesPriority ? event.sourceOrdinal() : old.priorityOrdinal;
            RestingOrder updated = new RestingOrder(
                old.orderId, old.side, event.priceNano(), event.size(), priorityOrdinal
            );
            orders.put(updated.orderId, updated);
            PriceLevel newLevel = level(updated.side, updated.priceNano, true);
            if (losesPriority) {
                // 改价或增量后，订单以新的优先级重新进入该价位队列末尾。
                newLevel.orders.put(updated.orderId, updated);
            } else {
                // 同价减量不改变时间优先级，仍保留原始入队顺序。
                insertByPriority(newLevel, updated);
            }
        }

        private void insertByPriority(PriceLevel level, RestingOrder updated) {
            LinkedHashMap<Long, RestingOrder> rebuilt = new LinkedHashMap<>();
            boolean inserted = false;
            for (RestingOrder order : level.orders.values()) {
                if (!inserted && Long.compareUnsigned(updated.priorityOrdinal, order.priorityOrdinal) < 0) {
                    rebuilt.put(updated.orderId, updated);
                    inserted = true;
                }
                rebuilt.put(order.orderId, order);
            }
            if (!inserted) {
                rebuilt.put(updated.orderId, updated);
            }
            level.orders.clear();
            level.orders.putAll(rebuilt);
        }

        private void cancel(MboEvent event) {
            requireBookSide(event);
            RestingOrder old = requireOrder(event.orderId(), "Cancel");
            if (old.side != event.side() || old.priceNano != event.priceNano()) {
                throw new MboBookInvariantException("Cancel side/price differs from active order");
            }
            if (event.size() == 0 || event.size() > old.size) {
                throw new MboBookInvariantException("invalid Cancel size");
            }

            PriceLevel priceLevel = level(old.side, old.priceNano, false);
            if (event.size() == old.size) {
                orders.remove(old.orderId);
                priceLevel.orders.remove(old.orderId);
                removeLevelIfEmpty(old.side, old.priceNano, priceLevel);
            } else {
                RestingOrder reduced = new RestingOrder(
                    old.orderId, old.side, old.priceNano, old.size - event.size(), old.priorityOrdinal
                );
                orders.put(reduced.orderId, reduced);
                priceLevel.orders.put(reduced.orderId, reduced);
            }
        }

        private void delete(LiveMboEvent event) {
            requireBookSide(event.side());
            RestingOrder old = requireOrder(event.orderId(), "Delete");
            if (old.side != event.side() || old.priceNano != event.priceNano()) {
                throw new MboBookInvariantException("Delete side/price differs from active order");
            }
            PriceLevel priceLevel = level(old.side, old.priceNano, false);
            orders.remove(old.orderId);
            priceLevel.orders.remove(old.orderId);
            removeLevelIfEmpty(old.side, old.priceNano, priceLevel);
        }

        private void clear() {
            orders.clear();
            bids.clear();
            asks.clear();
        }

        private RestingOrder requireOrder(long orderId, String action) {
            RestingOrder order = orders.get(orderId);
            if (order == null) {
                throw new MboBookInvariantException(
                    action + " references unknown order_id=" + Long.toUnsignedString(orderId)
                );
            }
            return order;
        }

        private List<OrderView> ordersAtLevel(char side, long priceNano) {
            PriceLevel level = (side == 'B' ? bids : asks).get(priceNano);
            if (level == null) {
                return List.of();
            }
            List<OrderView> result = new ArrayList<>(level.orders.size());
            for (RestingOrder order : level.orders.values()) {
                result.add(new OrderView(order.orderId, order.side, order.priceNano, order.size, order.priorityOrdinal));
            }
            return List.copyOf(result);
        }

        private BookSnapshot snapshot(BookKey key, MboEvent event, int depth) {
            return snapshot(
                key, event.sourceOrdinal(), event.tsEventNs(), event.tsRecvNs(), event.sequence(),
                event.action(), event.side(), depth
            );
        }

        private BookSnapshot snapshot(
            BookKey key,
            long sourceOrdinal,
            long tsEventNs,
            long tsRecvNs,
            long sequence,
            char action,
            char side,
            int depth
        ) {
            List<Level> bidLevels = aggregate(bids.descendingMap(), depth);
            List<Level> askLevels = aggregate(asks, depth);
            // 非严格模式保留交叉盘，作为可审计的异常信号；严格模式则在 F_LAST 边界处
            // 由调用方抛错，禁止将异常快照继续持久化或推送。
            boolean crossed = !bidLevels.isEmpty() && !askLevels.isEmpty()
                && bidLevels.getFirst().priceNano() >= askLevels.getFirst().priceNano();
            return new BookSnapshot(
                key, sourceOrdinal, tsEventNs, tsRecvNs, sequence, action, side,
                bidLevels, askLevels, crossed
            );
        }

        private static List<Level> aggregate(NavigableMap<Long, PriceLevel> levels, int depth) {
            List<Level> result = new ArrayList<>(Math.min(depth, levels.size()));
            for (Map.Entry<Long, PriceLevel> entry : levels.entrySet()) {
                long size = 0;
                for (RestingOrder order : entry.getValue().orders.values()) {
                    size = Math.addExact(size, order.size);
                }
                result.add(new Level(entry.getKey(), size, entry.getValue().orders.size()));
                if (result.size() == depth) {
                    break;
                }
            }
            return List.copyOf(result);
        }

        private PriceLevel level(char side, long priceNano, boolean create) {
            NavigableMap<Long, PriceLevel> levels = side == 'B' ? bids : asks;
            PriceLevel result = levels.get(priceNano);
            if (result == null && create) {
                result = new PriceLevel();
                levels.put(priceNano, result);
            }
            if (result == null) {
                throw new MboBookInvariantException("missing price level");
            }
            return result;
        }

        private void removeLevelIfEmpty(char side, long priceNano, PriceLevel priceLevel) {
            if (priceLevel.orders.isEmpty()) {
                (side == 'B' ? bids : asks).remove(priceNano);
            }
        }

        private static void requireBookSide(MboEvent event) {
            requireBookSide(event.side());
        }

        private static void requireBookSide(char side) {
            if (side != 'B' && side != 'A') {
                throw new MboBookInvariantException("book action requires bid/ask side");
            }
        }
    }

    private static final class PriceLevel {
        private final LinkedHashMap<Long, RestingOrder> orders = new LinkedHashMap<>();
    }

    private static final class RestingOrder {
        private final long orderId;
        private final char side;
        private final long priceNano;
        private final long size;
        private final long priorityOrdinal;

        private RestingOrder(long orderId, char side, long priceNano, long size, long priorityOrdinal) {
            this.orderId = orderId;
            this.side = side;
            this.priceNano = priceNano;
            this.size = size;
            this.priorityOrdinal = priorityOrdinal;
        }
    }
}
