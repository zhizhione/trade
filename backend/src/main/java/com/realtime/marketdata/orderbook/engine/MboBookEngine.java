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
    /** 对外快照每侧最多返回的价位数。 */
    public static final int MAX_DEPTH = 400;
    /** 默认快照深度。 */
    public static final int DEFAULT_DEPTH = MAX_DEPTH;
    /**
     * @deprecated 快照已统一限制为 {@link #MAX_DEPTH}，该常量仅为兼容旧调用方保留。
     */
    @Deprecated
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

    /** 创建默认实时引擎：发现交叉盘即失败，适用于生产校验和执行场景。 */
    public MboBookEngine() {
        this(true, DEFAULT_DEPTH);
    }

    /**
     * 验证和研究调用方可关闭实时交叉盘拒绝，以保留带有 {@code crossed} 标记的异常快照；
     * 历史事件始终按源记录逐条应用，不再启用独立的严格历史事务模式。
     */
    public MboBookEngine(boolean rejectCrossedBooks) {
        this(rejectCrossedBooks, DEFAULT_DEPTH);
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
        BookSnapshot snapshot = applyHistoricalBook(
            key, books.computeIfAbsent(key, ignored -> new OrderBook()), event
        );
        hasAppliedEvent = true;
        lastSourceOrdinal = event.sourceOrdinal();
        return Optional.ofNullable(snapshot);
    }

    /**
     * 应用一条归一化后的实时 L3 更新。与 DBN 数据不同，每条实时更新均视为完整消息，
     * 因此每次状态更新后都会返回快照。
     */
    public Optional<BookSnapshot> apply(LiveMboEvent event) {
        requireStrictlyIncreasingOrdinal(event.sourceOrdinal());

        BookKey key = new BookKey(event.publisherId(), event.instrumentId());
        OrderBook book = books.computeIfAbsent(key, ignored -> new OrderBook());
        BookSnapshot snapshot = applyLiveBook(key, book, event);
        hasAppliedEvent = true;
        lastSourceOrdinal = event.sourceOrdinal();
        return Optional.of(snapshot);
    }

    private BookSnapshot applyHistoricalBook(BookKey key, OrderBook book, MboEvent event) {
        book.apply(event);
        if ((event.flags() & F_LAST) == 0) {
            return null;
        }
        BookSnapshot snapshot = book.snapshot(key, event, snapshotDepth);
        return snapshot;
    }

    private BookSnapshot applyLiveBook(BookKey key, OrderBook book, LiveMboEvent event) {
        if (rejectCrossedBooks && createsCrossing(event.action())) {
            OrderBook candidate = book.copy();
            candidate.apply(event);
            requireNotCrossed(candidate.snapshot(
                key,
                event.sourceOrdinal(),
                event.tsEventNs(),
                event.tsRecvNs(),
                event.sequence(),
                event.action().snapshotAction(),
                event.side(),
                snapshotDepth
            ), event.sourceOrdinal());
            book.apply(event);
        } else {
            book.apply(event);
        }
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
        if (rejectCrossedBooks) {
            requireNotCrossed(snapshot, event.sourceOrdinal());
        }
        return snapshot;
    }

    private static boolean createsCrossing(LiveMboEvent.Action action) {
        return action == LiveMboEvent.Action.ADD || action == LiveMboEvent.Action.MODIFY;
    }

    private static void requireNotCrossed(BookSnapshot snapshot, long sourceOrdinal) {
        if (snapshot.crossed()) {
            throw new MboBookInvariantException(
                "crossed book at sourceOrdinal=" + Long.toUnsignedString(sourceOrdinal)
            );
        }
    }

    /**
     * 返回指定订单簿的当前可观测状态。若从未接收过该簿的事件，则返回空快照而非空值。
     */
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
        BookKey key = new BookKey(publisherId, instrumentId);
        OrderBook book = books.get(key);
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
                case ADD -> add(event);
                case MODIFY -> modify(event);
                case CANCEL -> cancel(asHistoricalEvent(event, 'C'));
                case DELETE -> delete(event);
                case CLEAR -> clear();
                case TRADE, FILL, NOOP -> { }
            }
        }

        private OrderBook copy() {
            OrderBook copy = new OrderBook();
            for (PriceLevel sourceLevel : bids.values()) {
                for (RestingOrder sourceOrder : sourceLevel.orders.values()) {
                    copyOrder(copy, sourceOrder);
                }
            }
            for (PriceLevel sourceLevel : asks.values()) {
                for (RestingOrder sourceOrder : sourceLevel.orders.values()) {
                    copyOrder(copy, sourceOrder);
                }
            }
            return copy;
        }

        private static void copyOrder(OrderBook target, RestingOrder source) {
            RestingOrder order = new RestingOrder(
                source.orderId, source.side, source.priceNano, source.size, source.priorityOrdinal
            );
            target.orders.put(order.orderId, order);
            target.level(order.side, order.priceNano, true).orders.put(order.orderId, order);
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

        private void add(LiveMboEvent event) {
            requireBookSide(event.side());
            if (event.size() == 0) {
                throw new MboBookInvariantException("zero-size Add");
            }
            if (orders.containsKey(event.orderId())) {
                throw new MboBookInvariantException(
                    "duplicate active order_id=" + Long.toUnsignedString(event.orderId())
                );
            }
            long priority = event.priority() == null ? event.sourceOrdinal() : event.priority();
            RestingOrder order = new RestingOrder(
                event.orderId(), event.side(), event.priceNano(), event.size(), priority
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

        private void modify(LiveMboEvent event) {
            requireBookSide(event.side());
            if (event.size() == 0) {
                throw new MboBookInvariantException("zero-size Modify");
            }
            RestingOrder old = requireOrder(event.orderId(), "Modify");
            if (old.side != event.side()) {
                throw new MboBookInvariantException("Modify changed side");
            }

            boolean priorityChanged = event.priority() != null
                && Long.compareUnsigned(event.priority(), old.priorityOrdinal) != 0;
            if (old.priceNano == event.priceNano() && old.size == event.size() && !priorityChanged) {
                // An explicit unchanged provider priority is a semantic no-op. Replacing the
                // value in-place preserves its exact position among equal-priority orders.
                RestingOrder unchanged = new RestingOrder(
                    old.orderId, old.side, old.priceNano, old.size, old.priorityOrdinal
                );
                orders.put(unchanged.orderId, unchanged);
                level(old.side, old.priceNano, false).orders.put(unchanged.orderId, unchanged);
                return;
            }
            boolean losesPriority = old.priceNano != event.priceNano() || old.size < event.size()
                || priorityChanged;
            PriceLevel oldLevel = level(old.side, old.priceNano, false);
            oldLevel.orders.remove(old.orderId);
            removeLevelIfEmpty(old.side, old.priceNano, oldLevel);

            long priority = event.priority() != null
                ? event.priority()
                : (losesPriority ? event.sourceOrdinal() : old.priorityOrdinal);
            RestingOrder updated = new RestingOrder(
                old.orderId, old.side, event.priceNano(), event.size(), priority
            );
            orders.put(updated.orderId, updated);
            PriceLevel newLevel = level(updated.side, updated.priceNano, true);
            if (losesPriority) {
                if (event.priority() == null) {
                    newLevel.orders.put(updated.orderId, updated);
                } else {
                    insertByPriority(newLevel, updated);
                }
            } else {
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
            RestingOrder old = requireOrder(event.orderId(), "Delete");
            if (event.side() != 'N' && old.side != event.side()) {
                throw new MboBookInvariantException("Delete side differs from active order");
            }
            if (event.priceNano() != LiveMboEvent.MISSING_PRICE_NANO
                && old.priceNano != event.priceNano()) {
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
            // 交叉盘作为可审计的异常信号保留；实时严格校验在状态迁移前由调用方完成。
            boolean crossed = !bidLevels.isEmpty() && !askLevels.isEmpty()
                && bidLevels.getFirst().priceNano() >= askLevels.getFirst().priceNano();
            return new BookSnapshot(
                key, sourceOrdinal, tsEventNs, tsRecvNs, sequence, action, side,
                bidLevels, askLevels, crossed
            );
        }

        private static List<Level> aggregate(NavigableMap<Long, PriceLevel> levels, int depth) {
            int effectiveDepth = Math.min(depth, MAX_DEPTH);
            List<Level> result = new ArrayList<>(Math.min(effectiveDepth, levels.size()));
            for (Map.Entry<Long, PriceLevel> entry : levels.entrySet()) {
                long size = 0;
                for (RestingOrder order : entry.getValue().orders.values()) {
                    size = Math.addExact(size, order.size);
                }
                result.add(new Level(entry.getKey(), size, entry.getValue().orders.size()));
                if (result.size() == effectiveDepth) {
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
