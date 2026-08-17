package com.realtime.marketdata.mbo.processor;

/**
 * 输入到同一 MBO 状态机的一条有序来源流的身份标识。
 *
 * <p>历史回放通常以 DBN 文件哈希作为 {@code streamId}；实时连接则使用稳定的
 * source_stream_id。来源序号可能在重连后从头开始，因此不同键绝不能共用同一个引擎实例。</p>
 */
public record MboStreamKey(String source, String streamId) {
    public MboStreamKey {
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("MBO source is required");
        }
        if (streamId == null || streamId.isBlank()) {
            throw new IllegalArgumentException("MBO streamId is required");
        }
    }
}
