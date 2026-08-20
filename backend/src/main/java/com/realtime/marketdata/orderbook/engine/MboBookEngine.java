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

    /** 创建默认生产引擎：保留交叉盘并在快照中设置 {@code crossed=true}。 */
    public MboBookEngine() {
        this(false, DEFAULT_DEPTH);
    }

    /**
     * 调用方可选择是否拒绝交叉盘。历史和实时生产回放均可关闭该拒绝并保留
     * {@code crossed} 标记；需要严格校验的审计调用方可显式开启。
     */
    public MboBookEngine(boolean rejectCrossedBooks) {
        this(rejectCrossedBooks, DEFAULT_DEPTH);
    }

    public MboBookEngine(boolean rejectCrossedBooks, int snapshotDepth) {
        validateDepth(snapshotDepth);
        this.rejectCrossedBooks = rejectCrossedBooks;
        this.snapshotDepth = snapshotDepth;
    }

    /** 将 Databento 原始事件适配为统一订单簿更新。 */
    public Optional<BookSnapshot> apply(MboEvent event) {
        return apply(BookUpdate.fromDatabento(event));
    }

    /** 将实时供应商事件适配为统一订单簿更新。 */
    public Optional<BookSnapshot> apply(LiveMboEvent event) {
        return apply(BookUpdate.fromLive(event));
    }

    /**
     * 应用一条规范化订单簿更新。历史 Databento 与实时 ATAS 最终都只经过这一套状态迁移、
     * 队列优先级、交叉盘判断和快照生成逻辑；来源差异只存在于 {@link BookUpdate} 适配阶段。
     */
    public Optional<BookSnapshot> apply(BookUpdate event) {
        requireStrictlyIncreasingOrdinal(event.sourceOrdinal());
        if ((event.flags() & (F_TOB | F_MBP)) != 0) {
            throw new MboBookInvariantException("MBO engine rejects F_TOB/F_MBP records");
        }

        BookKey key = event.key();
        OrderBook book = books.computeIfAbsent(key, ignored -> new OrderBook());
        if (rejectCrossedBooks && createsCrossing(event.action())) {
            OrderBook candidate = book.copy();
            candidate.apply(event);
            requireNotCrossed(candidate.snapshot(event, snapshotDepth), event.sourceOrdinal());
        }
        book.apply(event);
        BookSnapshot snapshot = book.snapshot(event, snapshotDepth);
        if (rejectCrossedBooks) {
            requireNotCrossed(snapshot, event.sourceOrdinal());
        }
        hasAppliedEvent = true;
        lastSourceOrdinal = event.sourceOrdinal();
        return Optional.of(snapshot);
    }

    private static boolean createsCrossing(BookUpdate.Action action) {
        return action == BookUpdate.Action.ADD || action == BookUpdate.Action.MODIFY;
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
            ? new BookSnapshot(key, -1L, 0L, 0L, 0L, 'N', 'N', List.of(), List.of(), false, false)
            : book.snapshot(key, depth);
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

    /**
     * 与来源协议无关的一条订单簿更新。Databento/ATAS 只在进入引擎前映射到这里，避免核心
     * 状态机为每种来源维护一套几乎相同的动作逻辑。
     */
    public record BookUpdate(
        long sourceOrdinal,
        long tsEventNs,
        long tsRecvNs,
        BookKey key,
        Action action,
        char side,
        long priceNano,
        long size,
        long orderId,
        long sequence,
        Long priority,
        int flags
    ) {
        public BookUpdate {
            if (key == null) throw new IllegalArgumentException("book key is required");
            if (action == null) throw new IllegalArgumentException("book action is required");
        }

        /**
         * Returns the provenance of the queue priority used by the engine.  Databento MBO
         * records do not carry the ATAS queue-priority field, so a null priority is deliberately
         * represented as a deterministic source-order fallback rather than as a native priority.
         */
        public PrioritySource prioritySource() {
            return priority == null ? PrioritySource.SOURCE_ORDINAL_FALLBACK : PrioritySource.NATIVE;
        }

        public enum PrioritySource {
            NATIVE,
            SOURCE_ORDINAL_FALLBACK
        }

        public static BookUpdate fromDatabento(MboEvent event) {
            return new BookUpdate(
                event.sourceOrdinal(), event.tsEventNs(), event.tsRecvNs(),
                new BookKey(event.publisherId(), event.instrumentId()),
                Action.fromDatabento(event.action()), event.side(), event.priceNano(), event.size(),
                event.orderId(), event.sequence(), null, event.flags()
            );
        }

        public static BookUpdate fromLive(LiveMboEvent event) {
            return new BookUpdate(
                event.sourceOrdinal(), event.tsEventNs(), event.tsRecvNs(),
                new BookKey(event.publisherId(), event.instrumentId()),
                Action.fromLive(event.action()), event.side(), event.priceNano(), event.size(),
                event.orderId(), event.sequence(), event.priority(), 0
            );
        }

        public enum Action {
            ADD('A'),
            MODIFY('M'),
            CANCEL('C'),
            DELETE('C'),
            CLEAR('R'),
            TRADE('T'),
            FILL('F'),
            NOOP('N');

            private final char snapshotAction;

            Action(char snapshotAction) {
                this.snapshotAction = snapshotAction;
            }

            public char snapshotAction() {
                return snapshotAction;
            }

            private static Action fromDatabento(char action) {
                return switch (action) {
                    case 'A' -> ADD;
                    case 'M' -> MODIFY;
                    case 'C' -> CANCEL;
                    case 'R' -> CLEAR;
                    case 'T' -> TRADE;
                    case 'F' -> FILL;
                    case 'N' -> NOOP;
                    default -> throw new MboBookInvariantException("unsupported MBO action=" + action);
                };
            }

            private static Action fromLive(LiveMboEvent.Action action) {
                return switch (action) {
                    case ADD -> ADD;
                    case MODIFY -> MODIFY;
                    case CANCEL -> CANCEL;
                    case DELETE -> DELETE;
                    case CLEAR -> CLEAR;
                    case TRADE -> TRADE;
                    case FILL -> FILL;
                    case NOOP -> NOOP;
                };
            }
        }
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
        boolean crossed,
        boolean locked
    ) {
        /** Backwards-compatible constructor for callers compiled against the crossed-only contract. */
        public BookSnapshot(
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
            this(key, sourceOrdinal, tsEventNs, tsRecvNs, sequence, action, side,
                bids, asks, crossed, false);
        }

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

        private void apply(BookUpdate event) {
            switch (event.action()) {
                case ADD -> add(event);
                case MODIFY -> modify(event);
                case CANCEL -> cancel(event);
                case DELETE -> delete(event);
                case CLEAR -> clear();
                // 成交、成交填充和无操作记录携带成交或分帧信息，但不会直接变更挂单表。
                // 若某交易所明确规定 fill 应扣减剩余量，可在后续接入层加入对应策略。
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

        private void add(BookUpdate event) {
            requireBookSide(event.side());
            if (event.size() <= 0) {
                throw new MboBookInvariantException("zero-size Add");
            }
            if (orders.containsKey(event.orderId())) {
                throw new MboBookInvariantException(
                    "duplicate active order_id=" + Long.toUnsignedString(event.orderId())
                );
            }
            // sourceOrdinal identifies raw position. Queue priority is separate; when absent,
            // the strictly ordered source position is the deterministic queue-order fallback.
            long priority = event.priority() == null ? event.sourceOrdinal() : event.priority();
            RestingOrder order = new RestingOrder(
                event.orderId(), event.side(), event.priceNano(), event.size(), priority
            );
            orders.put(order.orderId, order);
            level(order.side, order.priceNano, true).orders.put(order.orderId, order);
        }

        private void modify(BookUpdate event) {
            requireBookSide(event.side());
            if (event.size() <= 0) {
                throw new MboBookInvariantException("zero-size Modify");
            }
            RestingOrder old = requireOrder(event.orderId(), "Modify");
            if (old.side != event.side()) {
                throw new MboBookInvariantException("Modify changed side");
            }

            // CME 风格队列规则：改价或增量会失去原有时间优先级；同价减量保留原队列位置。
            boolean priorityChanged = event.priority() != null
                && Long.compareUnsigned(event.priority(), old.priorityOrdinal) != 0;
            boolean losesPriority = old.priceNano != event.priceNano() || old.size < event.size()
                || priorityChanged;
            PriceLevel oldLevel = level(old.side, old.priceNano, false);
            oldLevel.orders.remove(old.orderId);
            removeLevelIfEmpty(old.side, old.priceNano, oldLevel);

            long priorityOrdinal = event.priority() != null
                ? event.priority()
                : (losesPriority ? event.sourceOrdinal() : old.priorityOrdinal);
            RestingOrder updated = new RestingOrder(
                old.orderId, old.side, event.priceNano(), event.size(), priorityOrdinal
            );
            orders.put(updated.orderId, updated);
            PriceLevel newLevel = level(updated.side, updated.priceNano, true);
            if (losesPriority) {
                if (event.priority() == null) {
                    // 没有来源 priority 时，规范化 source ordinal 作为队尾回退。
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

        private void cancel(BookUpdate event) {
            requireBookSide(event.side());
            RestingOrder old = requireOrder(event.orderId(), "Cancel");
            if (old.side != event.side() || old.priceNano != event.priceNano()) {
                throw new MboBookInvariantException("Cancel side/price differs from active order");
            }
            if (event.size() <= 0 || event.size() > old.size) {
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

        private void delete(BookUpdate event) {
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

        private BookSnapshot snapshot(BookKey key, int depth) {
            return snapshot(new BookUpdate(
                -1L, 0L, 0L, key, BookUpdate.Action.NOOP, 'N', 0L, 0L, 0L, 0L, null, 0
            ), depth);
        }

        private BookSnapshot snapshot(BookUpdate event, int depth) {
            List<Level> bidLevels = aggregate(bids.descendingMap(), depth);
            List<Level> askLevels = aggregate(asks, depth);
            // 交叉盘作为可审计的异常信号保留；严格模式由统一 apply 在提交前校验候选簿。
            boolean locked = !bidLevels.isEmpty() && !askLevels.isEmpty()
                && bidLevels.getFirst().priceNano() == askLevels.getFirst().priceNano();
            boolean crossed = !bidLevels.isEmpty() && !askLevels.isEmpty()
                && bidLevels.getFirst().priceNano() >= askLevels.getFirst().priceNano();
            return new BookSnapshot(
                event.key(), event.sourceOrdinal(), event.tsEventNs(), event.tsRecvNs(), event.sequence(),
                event.action().snapshotAction(), event.side(),
                bidLevels, askLevels, crossed, locked
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
