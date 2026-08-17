package com.realtime.marketdata.market.port;

import com.realtime.marketdata.core.event.MarketEvent;
import com.realtime.marketdata.marketstate.model.MarketSnapshot;

/**
 * 向外发布归一化事件和当前市场状态的输出端口。WebSocket、消息队列或测试中的内存
 * 收集器都可以实现该接口，行情处理流程无需感知下游传输方式。
 */
public interface MarketUpdatePublisher {
    /** 发布归一化事件，适合事件列表、审计订阅或其他实时消费者。 */
    void broadcastEvent(MarketEvent event);

    /** 发布当前展示快照；它是派生状态，不能替代原始 MBO 回放数据。 */
    void broadcastSnapshot(MarketSnapshot snapshot);
}
