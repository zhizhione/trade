package com.realtime.marketdata.market.port;

import com.realtime.marketdata.core.event.MarketEvent;

/**
 * 归一化行情事件的持久化端口。处理器通过该端口保存结果，避免直接耦合某一种数据库、
 * JDBC 驱动或失败重试策略。
 */
public interface MarketEventStore {
    /** 保存已通过身份校验的归一化事件；实现可按来源写入对应原始事实表。 */
    void save(MarketEvent event);

}
