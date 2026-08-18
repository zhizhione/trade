package com.realtime.marketdata.replay.model;

import java.util.List;

public record ReplaySession(
    String fileSha256,
    int publisherId,
    long instrumentId,
    String symbol,
    int bucketMs,
    int depth,
    int barIntervalMs,
    List<ReplayBar> bars,
    List<ReplayFrame> frames,
    Long nextStartMs
) {
}
