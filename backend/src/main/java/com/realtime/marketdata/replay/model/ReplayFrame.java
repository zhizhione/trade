package com.realtime.marketdata.replay.model;

import java.util.List;

/**
 * 面向浏览器的单个回放帧。价格通过 JSON 以精确纳元整数传输，由前端按合约最小跳动
 * 格式化；这样不会因为 JavaScript 浮点数参与聚合而改变盘口。
 */
public record ReplayFrame(
    long timeMs,
    long sourceOrdinal,
    long sequence,
    int bucketMs,
    long addedSize,
    long cancelledSize,
    long tradedSize,
    List<DepthLevel> bids,
    List<DepthLevel> asks,
    boolean complete,
    boolean crossed,
    boolean locked
) {
    /** Compatibility constructor for the crossed-only replay payload. */
    public ReplayFrame(
        long timeMs,
        long sourceOrdinal,
        long sequence,
        int bucketMs,
        long addedSize,
        long cancelledSize,
        long tradedSize,
        List<DepthLevel> bids,
        List<DepthLevel> asks,
        boolean complete,
        boolean crossed
    ) {
        this(timeMs, sourceOrdinal, sequence, bucketMs, addedSize, cancelledSize, tradedSize,
            bids, asks, complete, crossed, false);
    }

    public ReplayFrame {
        bids = List.copyOf(bids);
        asks = List.copyOf(asks);
    }

    public record DepthLevel(long priceNano, long size, int orderCount) {
    }
}
