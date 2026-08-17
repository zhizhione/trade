package com.realtime.marketdata.replay.source;

/** 表示历史回放只读存储暂时不可用。 */
public final class ReplayDataAccessException extends IllegalStateException {

    public ReplayDataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
