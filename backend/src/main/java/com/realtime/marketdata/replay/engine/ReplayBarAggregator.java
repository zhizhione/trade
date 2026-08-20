package com.realtime.marketdata.replay.engine;

import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayFrame;
import java.util.ArrayList;
import java.util.List;

/** Shared midpoint-bar aggregation for REST sessions and interactive WebSocket replay. */
public final class ReplayBarAggregator {
    private final int intervalMs;
    private ReplayBar current;

    public ReplayBarAggregator(int intervalMs) {
        if (intervalMs < 100 || intervalMs > 3_600_000) {
            throw new IllegalArgumentException("barIntervalMs must be between 100 and 3600000");
        }
        this.intervalMs = intervalMs;
    }

    /** Observes one frame and returns a completed bar when its bucket changes. */
    public ReplayBar observe(ReplayFrame frame) {
        if (!isUsable(frame)) {
            return null;
        }
        long midpoint = Math.floorDiv(
            Math.addExact(frame.bids().getFirst().priceNano(), frame.asks().getFirst().priceNano()), 2
        );
        long bucket = Math.multiplyExact(Math.floorDiv(frame.timeMs(), intervalMs), intervalMs);
        ReplayBar completed = null;
        if (current == null || current.timeMs() != bucket) {
            completed = current;
            current = new ReplayBar(bucket, midpoint, midpoint, midpoint, midpoint);
        } else {
            current = new ReplayBar(
                current.timeMs(), current.openNano(), Math.max(current.highNano(), midpoint),
                Math.min(current.lowNano(), midpoint), midpoint
            );
        }
        return completed;
    }

    public ReplayBar current() {
        return current;
    }

    public ReplayBar finish() {
        ReplayBar result = current;
        current = null;
        return result;
    }

    public static List<ReplayBar> aggregate(List<ReplayFrame> frames, int intervalMs) {
        ReplayBarAggregator aggregator = new ReplayBarAggregator(intervalMs);
        List<ReplayBar> result = new ArrayList<>();
        for (ReplayFrame frame : frames) {
            ReplayBar completed = aggregator.observe(frame);
            if (completed != null) result.add(completed);
        }
        ReplayBar tail = aggregator.finish();
        if (tail != null) result.add(tail);
        return List.copyOf(result);
    }

    private static boolean isUsable(ReplayFrame frame) {
        return frame.complete() && !frame.crossed() && !frame.locked()
            && !frame.bids().isEmpty() && !frame.asks().isEmpty();
    }
}
