package com.realtime.marketdata.replay.engine;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import com.realtime.marketdata.orderbook.engine.MboBookInvariantException;
import com.realtime.marketdata.replay.model.ReplayFrame;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 在每个事件时间采样桶内保留最后一个 F_LAST 后的完整订单簿状态。
 *
 * <p>订单簿只有处理过首条 Clear 后才可视为完整；此前快照仍保留用于诊断，但会标记为
 * 不可执行，防止策略在未知初始状态上交易。</p>
 */
public final class MboReplaySampler {
    private final int bucketMs;
    private final long bucketNs;
    private final long windowStartMs;
    private final MboBookEngine engine;
    private final Map<MboBookEngine.BookKey, Pending> pending = new HashMap<>();
    private final Map<MboBookEngine.BookKey, BucketFlow> flows = new HashMap<>();
    private final Set<MboBookEngine.BookKey> initializedBooks = new HashSet<>();

    /** 按回放策略创建采样器；默认使用历史深度和允许保留交叉盘的策略。 */
    public MboReplaySampler(int bucketMs, int depth, boolean rejectCrossedBooks) {
        this(bucketMs, depth, rejectCrossedBooks, Long.MIN_VALUE);
    }

    /**
     * {@code windowStartMs} 之前的事件仍须用于预热 L3 状态，但不能计入用户可见的新增、
     * 撤单和成交统计，否则回放窗口首帧的流量指标会被历史事件污染。
     */
    public MboReplaySampler(
        int bucketMs,
        int depth,
        boolean rejectCrossedBooks,
        long windowStartMs
    ) {
        this(bucketMs, new MboBookEngineFactory().create(rejectCrossedBooks, depth), windowStartMs);
    }

    /** 使用已按场景选择策略的订单簿引擎创建采样器。 */
    /** 使用调用方提供的订单簿引擎创建采样器，便于测试或特殊校验策略复用。 */
    public MboReplaySampler(int bucketMs, MboBookEngine engine, long windowStartMs) {
        if (bucketMs < 1 || bucketMs > 60_000) {
            throw new IllegalArgumentException("bucketMs must be between 1 and 60000");
        }
        this.bucketMs = bucketMs;
        this.bucketNs = Math.multiplyExact((long) bucketMs, 1_000_000L);
        this.windowStartMs = windowStartMs;
        this.engine = engine;
    }

    /**
     * 按来源顺序应用一条事件，并返回因时间桶推进而已最终确定的采样帧。
     *
     * <p>只有 {@code F_LAST} 可结束一条消息，因此同一消息中的中间 MBO 更新仅停留在内存
     * 引擎中。当事件时间跨入新桶时，前一桶暂存帧会被发出，并由新桶的最新帧取代。</p>
     */
    /** 接收一条 MBO 事件，并输出因进入新时间桶而已经确定的旧桶帧。 */
    public List<ReplayFrame> accept(MboEvent event) {
        MboBookEngine.BookKey key = new MboBookEngine.BookKey(event.publisherId(), event.instrumentId());
        long bucketStartNs = bucketStart(event.tsEventNs());
        BucketFlow flow = flowFor(key, bucketStartNs);
        flow.record(event, isInDisplayWindow(event));
        if (event.action() == 'R') {
            initializedBooks.add(key);
        }
        var result = engine.apply(event);
        if (result.isEmpty()) {
            return List.of();
        }
        MboBookEngine.BookSnapshot snapshot = result.get();
        Pending previous = pending.get(key);
        if (previous != null && currentTimeMs(bucketStartNs) < previous.frame.timeMs()) {
            throw new MboBookInvariantException("F_LAST event timestamp regressed within book");
        }
        ReplayFrame current = frame(
            bucketStartNs, snapshot, initializedBooks.contains(key),
            flow.addedSize, flow.cancelledSize, flow.tradedSize
        );
        pending.put(key, new Pending(current));
        return previous != null && previous.frame.timeMs() < current.timeMs()
            ? List.of(previous.frame)
            : List.of();
    }

    /** 按源顺序号输出所有订单簿最后尚未结束的部分时间桶。 */
    /** 输入流结束时刷新每个订单簿的最后一个未完成时间桶。 */
    public List<ReplayFrame> finish() {
        List<ReplayFrame> result = new ArrayList<>();
        for (Pending value : pending.values()) {
            result.add(value.frame);
        }
        result.sort(Comparator.comparingLong(ReplayFrame::sourceOrdinal));
        pending.clear();
        flows.clear();
        return List.copyOf(result);
    }

    private long bucketStart(long tsEventNs) {
        if (tsEventNs < 0) {
            throw new MboBookInvariantException("replay sampler requires tsEventNs <= Long.MAX_VALUE");
        }
        return Math.multiplyExact(tsEventNs / bucketNs, bucketNs);
    }

    private BucketFlow flowFor(MboBookEngine.BookKey key, long bucketStartNs) {
        BucketFlow current = flows.get(key);
        if (current == null || current.bucketStartNs < bucketStartNs) {
            BucketFlow next = new BucketFlow(bucketStartNs);
            flows.put(key, next);
            return next;
        }
        if (current.bucketStartNs > bucketStartNs) {
            throw new MboBookInvariantException("event timestamp regressed within book flow bucket");
        }
        return current;
    }

    private ReplayFrame frame(
        long bucketStartNs,
        MboBookEngine.BookSnapshot snapshot,
        boolean complete,
        long addedSize,
        long cancelledSize,
        long tradedSize
    ) {
        return new ReplayFrame(
            currentTimeMs(bucketStartNs),
            snapshot.sourceOrdinal(),
            snapshot.sequence(),
            bucketMs,
            addedSize,
            cancelledSize,
            tradedSize,
            levels(snapshot.bids()),
            levels(snapshot.asks()),
            complete,
            snapshot.crossed()
        );
    }

    private long currentTimeMs(long bucketStartNs) {
        return Math.floorDiv(bucketStartNs, 1_000_000L);
    }

    private boolean isInDisplayWindow(MboEvent event) {
        return windowStartMs == Long.MIN_VALUE
            || Math.floorDiv(event.tsEventNs(), 1_000_000L) >= windowStartMs;
    }

    private List<ReplayFrame.DepthLevel> levels(List<MboBookEngine.Level> levels) {
        return levels.stream()
            .map(level -> new ReplayFrame.DepthLevel(level.priceNano(), level.size(), level.orderCount()))
            .toList();
    }

    private record Pending(ReplayFrame frame) {
    }

    private static final class BucketFlow {
        private final long bucketStartNs;
        private long addedSize;
        private long cancelledSize;
        private long tradedSize;

        private BucketFlow(long bucketStartNs) {
            this.bucketStartNs = bucketStartNs;
        }

        private void record(MboEvent event, boolean visible) {
            if (!visible) {
                return;
            }
            switch (event.action()) {
                case 'A' -> addedSize = Math.addExact(addedSize, event.size());
                case 'C' -> cancelledSize = Math.addExact(cancelledSize, event.size());
                case 'T' -> tradedSize = Math.addExact(tradedSize, event.size());
                // F 可能与 T 表示同一笔撮合的被动腿；若在此重复计数，会虚增成交量列。
                default -> { }
            }
        }
    }
}
