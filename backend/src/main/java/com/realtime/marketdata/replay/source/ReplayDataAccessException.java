package com.realtime.marketdata.replay.source;

/** 表示历史回放数据读取或订单簿重建不可用。 */
public final class ReplayDataAccessException extends IllegalStateException {

    public ReplayDataAccessException(String message) {
        super(message);
    }

    public ReplayDataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
