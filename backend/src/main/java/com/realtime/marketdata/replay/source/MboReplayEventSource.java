package com.realtime.marketdata.replay.source;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplayCursor;
import java.util.List;

/**
 * 供请求时历史 MBO 回放使用的只读事件源。
 *
 * <p>消费者返回 {@code false} 时可提前停止读取。该方法的布尔返回值用于告知调用方所选
 * 原始文件序列是否已完整消费；只有完整消费时，采样器最后暂存的时间桶才可以安全输出。</p>
 */
public interface MboReplayEventSource {
    /** 返回当前数据库中可供回放选择的文件、发布者和合约组合。 */
    List<ReplayCatalogEntry> catalog();

    /**
     * 按指定发布者和合约流式读取事件。实现必须保证源顺序稳定，并在消费者返回 false
     * 时立即停止，以便 WebSocket 停止或达到帧数上限时释放数据库资源。
     */
    boolean streamEvents(
        int publisherId,
        long instrumentId,
        long startMs,
        long endMs,
        MboEventConsumer consumer
    );

    /** 从指定原始位置继续读取；首次读取时 cursor 传 null。 */
    default StreamResult streamEvents(
        int publisherId,
        long instrumentId,
        long startMs,
        long endMs,
        ReplayCursor cursor,
        MboEventConsumer consumer
    ) {
        if (cursor != null) {
            throw new UnsupportedOperationException("replay source does not support cursor continuation");
        }
        return new StreamResult(
            streamEvents(publisherId, instrumentId, startMs, endMs, consumer),
            null
        );
    }

    /** 根据来源身份返回展示用合约代码；找不到时由实现决定是否返回回退名称。 */
    String symbol(int publisherId, long instrumentId);

    @FunctionalInterface
    interface MboEventConsumer {
        /** 消费一条已按源顺序读取的 MBO 事件，返回 false 表示停止读取。 */
        boolean accept(MboEvent event);
    }

    record StreamResult(boolean completed, ReplayCursor nextCursor) {
    }
}
