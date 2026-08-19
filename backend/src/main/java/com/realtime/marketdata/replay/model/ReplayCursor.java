package com.realtime.marketdata.replay.model;

/**
 * 原始 MBO 流的可恢复位置。时间戳不是可靠游标，因为多个事件可能共享同一时间，
 * 且 source ordinal 在不同文件中从零开始。
 */
public record ReplayCursor(String fileSha256, String sourceOrdinal, String lastEventNs) {
    public ReplayCursor {
        if (fileSha256 == null || fileSha256.isBlank()) {
            throw new IllegalArgumentException("cursor fileSha256 is required");
        }
        if (sourceOrdinal == null || sourceOrdinal.isBlank() || lastEventNs == null || lastEventNs.isBlank()) {
            throw new IllegalArgumentException("cursor position is required");
        }
    }
}
