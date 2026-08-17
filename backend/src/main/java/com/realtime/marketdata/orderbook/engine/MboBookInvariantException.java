package com.realtime.marketdata.orderbook.engine;

/** 当某条 MBO 事件将订单簿推进到非法状态时抛出，例如重复挂单、撤改单不存在或买卖盘交叉。 */
public class MboBookInvariantException extends RuntimeException {
    public MboBookInvariantException(String message) {
        super(message);
    }
}
