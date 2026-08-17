package com.realtime.marketdata.core.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.math.BigDecimal;
import java.time.Instant;
import tools.jackson.databind.JsonNode;

/**
 * 统一后的内存事件。``data`` 保存原始来源字段，价格和时间等
 * 顶层字段只为展示、快照和派生表提供便利，不能替代来源原始表。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MarketEvent(
    String eventId,
    String topic,
    String eventType,
    String source,
    Long canonicalId,
    String symbol,
    Instant eventTime,
    Instant receivedAt,
    Long sequence,
    BigDecimal price,
    BigDecimal quantity,
    String side,
    JsonNode data
) {
}
