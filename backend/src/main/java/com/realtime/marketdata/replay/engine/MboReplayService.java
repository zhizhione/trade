package com.realtime.marketdata.replay.engine;

import java.util.ArrayList;
import java.util.List;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplayFrame;
import com.realtime.marketdata.replay.model.ReplaySession;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MboReplayService {
    private static final long MAX_EPOCH_MILLIS = Long.MAX_VALUE / 1_000_000L;
    private static final MboBookEngineFactory ENGINE_FACTORY = new MboBookEngineFactory();
    private static final Logger log = LoggerFactory.getLogger(MboReplayService.class);

    private final MboReplayEventSource source;

    /** 使用只读事件源创建按请求重建历史订单簿的服务。 */
    public MboReplayService(MboReplayEventSource source) {
        this.source = source;
    }

    /** 返回可选回放目录；目录由持久化适配器按文件与合约身份展开。 */
    public List<ReplayCatalogEntry> catalog() {
        log.debug("Loading replay catalog");
        return source.catalog();
    }

    /**
     * 重建一个有限历史窗口。起始时间之前的事件用于预热订单簿，窗口内的完整消息转换为
     * 回放帧，并按指定周期从有效 BBO 中计算中间价 K 线。
     */
    public ReplaySession session(
        int publisherId,
        long instrumentId,
        int bucketMs,
        long startMs,
        long endMs,
        int limit,
        int barIntervalMs
    ) {
        return session(publisherId, instrumentId, bucketMs, startMs, endMs, limit, barIntervalMs, false);
    }

    /**
     * 重建历史窗口；诊断模式允许返回 400 档，否则默认限制为 100 档。
     */
    public ReplaySession session(
        int publisherId,
        long instrumentId,
        int bucketMs,
        long startMs,
        long endMs,
        int limit,
        int barIntervalMs,
        boolean diagnostic
    ) {
        long startedNanos = System.nanoTime();
        log.info(
            "Replay session started: publisherId={}, instrumentId={}, startMs={}, endMs={}, bucketMs={}, limit={}, barIntervalMs={}, depth={}",
            publisherId, instrumentId, startMs, endMs, bucketMs, limit, barIntervalMs,
            ReplayStreamRequest.depthFor(diagnostic)
        );
        if (publisherId < 0 || publisherId > 0xffff) {
            throw new IllegalArgumentException("publisherId outside UInt16");
        }
        if (instrumentId < 0 || instrumentId > 0xffff_ffffL) {
            throw new IllegalArgumentException("instrumentId outside UInt32");
        }
        if (bucketMs < 1 || bucketMs > 60_000 || startMs < 0 || endMs < startMs
            || startMs > MAX_EPOCH_MILLIS || endMs > MAX_EPOCH_MILLIS
            || limit < 1 || limit > 20_000) {
            throw new IllegalArgumentException("invalid replay range or limit");
        }
        if (barIntervalMs < 100 || barIntervalMs > 3_600_000) {
            throw new IllegalArgumentException("barIntervalMs must be between 100 and 3600000");
        }

        // 请求时间只是展示窗口，并非数据库可以直接定位的订单簿状态点。
        // 要得到 startMs 时刻正确的 L3 状态，必须按源顺序重放所选原始文件序列中此前的事件。
        MboReplaySampler sampler = new MboReplaySampler(
            bucketMs, ENGINE_FACTORY.create(false, ReplayStreamRequest.depthFor(diagnostic)), startMs
        );
        // 回放帧按时间桶采样。必须包含 startMs 所在桶，避免起始时刻之后的事件因桶起点
        // 早于请求时间而被错误丢弃。
        long displayStartMs = Math.multiplyExact(Math.floorDiv(startMs, bucketMs), bucketMs);
        ReplayWindowCollector collector = new ReplayWindowCollector(displayStartMs, endMs, limit);
        boolean exhausted = source.streamEvents(
            publisherId,
            instrumentId,
            startMs,
            endMs,
            event -> collector.accept(sampler.accept(event))
        );
        if (exhausted && !collector.hasNextPage() && !collector.reachedEnd()) {
            collector.accept(sampler.finish());
        }

        List<ReplayFrame> frames = collector.page();
        Long nextStartMs = null;
        if (!collector.reachedEnd() && collector.hasNextPage() && !frames.isEmpty()) {
            long candidate = Math.addExact(frames.getLast().timeMs(), bucketMs);
            if (candidate <= endMs) {
                nextStartMs = candidate;
            }
        }
        ReplaySession session = new ReplaySession(
            "MULTI_FILE",
            publisherId,
            instrumentId,
            source.symbol(publisherId, instrumentId),
            bucketMs,
            ReplayStreamRequest.depthFor(diagnostic),
            barIntervalMs,
            midpointBars(frames, barIntervalMs),
            frames,
            nextStartMs
        );
        log.info(
            "Replay session complete: publisherId={}, instrumentId={}, frames={}, bars={}, hasNextPage={}, elapsedMs={}",
            publisherId, instrumentId, frames.size(), session.bars().size(), nextStartMs != null,
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos)
        );
        return session;
    }

    static List<ReplayBar> midpointBars(List<ReplayFrame> frames, int intervalMs) {
        List<ReplayBar> result = new ArrayList<>();
        MutableBar current = null;
        for (ReplayFrame frame : frames) {
            if (!frame.complete() || frame.bids().isEmpty() || frame.asks().isEmpty() || frame.crossed()) {
                continue;
            }
            long midpoint = Math.floorDiv(
                Math.addExact(frame.bids().getFirst().priceNano(), frame.asks().getFirst().priceNano()),
                2
            );
            long bucket = Math.multiplyExact(Math.floorDiv(frame.timeMs(), intervalMs), intervalMs);
            if (current == null || current.timeMs != bucket) {
                if (current != null) {
                    result.add(current.freeze());
                }
                current = new MutableBar(bucket, midpoint);
            } else {
                current.observe(midpoint);
            }
        }
        if (current != null) {
            result.add(current.freeze());
        }
        return List.copyOf(result);
    }

    private static final class ReplayWindowCollector {
        private final long startMs;
        private final long endMs;
        private final int limit;
        private final List<ReplayFrame> frames;
        private boolean reachedEnd;

        private ReplayWindowCollector(long startMs, long endMs, int limit) {
            this.startMs = startMs;
            this.endMs = endMs;
            this.limit = limit;
            this.frames = new ArrayList<>(Math.min(limit + 1, 20_001));
        }

        private boolean accept(List<ReplayFrame> rows) {
            for (ReplayFrame frame : rows) {
                if (frame.timeMs() < startMs) {
                    continue;
                }
                if (frame.timeMs() > endMs) {
                    reachedEnd = true;
                    return false;
                }
                frames.add(frame);
                if (hasNextPage()) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasNextPage() {
            return frames.size() > limit;
        }

        private boolean reachedEnd() {
            return reachedEnd;
        }

        private List<ReplayFrame> page() {
            int pageSize = Math.min(frames.size(), limit);
            return List.copyOf(frames.subList(0, pageSize));
        }

    }

    private static final class MutableBar {
        private final long timeMs;
        private final long open;
        private long high;
        private long low;
        private long close;

        private MutableBar(long timeMs, long price) {
            this.timeMs = timeMs;
            this.open = price;
            this.high = price;
            this.low = price;
            this.close = price;
        }

        private void observe(long price) {
            high = Math.max(high, price);
            low = Math.min(low, price);
            close = price;
        }

        private ReplayBar freeze() {
            return new ReplayBar(timeMs, open, high, low, close);
        }
    }
}
