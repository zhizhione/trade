package com.realtime.marketdata.replay.model;

/**
 * 一根以最优买卖中点计算的 OHLC K 线。全部价格均使用精确纳元整数表示，序列化时
 * 不进行浮点换算，避免长时间回放中出现舍入偏差。
 */
public record ReplayBar(
    long timeMs,
    long openNano,
    long highNano,
    long lowNano,
    long closeNano
) {
}
