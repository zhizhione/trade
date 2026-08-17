package com.realtime.marketdata.marketstate.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * 供前端消费的最新状态视图，而不是可重放的订单簿真相。买卖档位由来源快照或最佳
 * 价字段覆盖更新，完整盘口重建仍应以原始 MBO 序列为准。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MarketSnapshot(
    String source,
    Long canonicalId,
    String sourceStreamId,
    String symbol,
    Instant eventTime,
    BigDecimal lastPrice,
    List<DepthLevel> bids,
    List<DepthLevel> asks,
    BigDecimal orderFlow,
    BigDecimal signalValue,
    String lastEventType
) {
    public record DepthLevel(BigDecimal price, BigDecimal quantity) {
    }
}
