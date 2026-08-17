package com.realtime.marketdata.replay.model;

/**
 * 可由客户端选择并按需重建的原始 MBO 数据流。每一项同时确定源文件、发布者和合约，
 * 防止在一个回放会话中混入不同订单簿的事件。
 */
public record ReplayCatalogEntry(
    String fileSha256,
    int publisherId,
    long instrumentId,
    String symbol,
    long startMs,
    long endMs,
    int bucketMs,
    long eventCount
) {
}
