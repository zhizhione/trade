package com.realtime.marketdata.replay.model;

/**
 * 一条由服务端驱动的历史回放流的请求参数。构造时完成时间范围、采样间隔和速度校验，
 * 使 WebSocket 执行阶段只处理已经合法的回放命令。普通模式固定返回 100 档，
 * 诊断模式固定返回 400 档，避免客户端任意放大单帧响应。
 */
public record ReplayStreamRequest(
    int publisherId,
    long instrumentId,
    int bucketMs,
    long startMs,
    long endMs,
    int barIntervalMs,
    double speed,
    boolean diagnostic
) {
    public static final int DEFAULT_DEPTH = 100;
    public static final int DIAGNOSTIC_DEPTH = 400;
    private static final long MAX_EPOCH_MILLIS = Long.MAX_VALUE / 1_000_000L;

    public ReplayStreamRequest(
        int publisherId,
        long instrumentId,
        int bucketMs,
        long startMs,
        long endMs,
        int barIntervalMs,
        double speed
    ) {
        this(publisherId, instrumentId, bucketMs, startMs, endMs, barIntervalMs, speed, false);
    }

    public ReplayStreamRequest {
        if (publisherId < 0 || publisherId > 0xffff) {
            throw new IllegalArgumentException("publisherId outside UInt16");
        }
        if (instrumentId < 0 || instrumentId > 0xffff_ffffL) {
            throw new IllegalArgumentException("instrumentId outside UInt32");
        }
        if (bucketMs < 1 || bucketMs > 60_000 || startMs < 0 || endMs < startMs
            || startMs > MAX_EPOCH_MILLIS || endMs > MAX_EPOCH_MILLIS) {
            throw new IllegalArgumentException("invalid replay time range");
        }
        if (barIntervalMs < 100 || barIntervalMs > 3_600_000) {
            throw new IllegalArgumentException("barIntervalMs must be between 100 and 3600000");
        }
        if (!Double.isFinite(speed) || speed <= 0 || speed > 1_000) {
            throw new IllegalArgumentException("speed must be between 0 and 1000");
        }
    }

    public int depth() {
        return depthFor(diagnostic);
    }

    public static int depthFor(boolean diagnostic) {
        return diagnostic ? DIAGNOSTIC_DEPTH : DEFAULT_DEPTH;
    }
}
