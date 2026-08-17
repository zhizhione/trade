package com.realtime.marketdata.mbo.model;

/**
 * Databento MBO 的 14 个原始字段，以及回放读取器补充的源顺序号。价格始终保持为
 * 固定点纳元整数，订单簿引擎不会将其转换为浮点数，以保证重建结果可重复且无精度损失。
 *
 * <p>{@code sourceOrdinal} 有意与 Databento 的 {@code sequence} 分离：后者可能在
 * 不同通道或连接会话中重置；前者才是 DBN 解码流内唯一、连续且可精确定位事件的位置。</p>
 */
public record MboEvent(
    long sourceOrdinal,
    long tsRecvNs,
    long tsEventNs,
    int rtype,
    int publisherId,
    long instrumentId,
    char action,
    char side,
    long priceNano,
    long size,
    int channelId,
    long orderId,
    int flags,
    int tsInDeltaNs,
    long sequence
) {
    /** 支持的 MBO 操作：新增、修改、撤单、清盘、成交、成交填充及无操作。 */
    private static final String ACTIONS = "AMCRTFN";
    /** Databento 方向：买方、卖方及无方向；无方向用于控制类或部分成交类记录。 */
    private static final String SIDES = "BAN";

    public MboEvent {
        requireUnsigned("sourceOrdinal", sourceOrdinal);
        requireUnsigned("tsRecvNs", tsRecvNs);
        requireUnsigned("tsEventNs", tsEventNs);
        requireRange("rtype", rtype, 0xffL);
        requireRange("publisherId", publisherId, 0xffffL);
        requireRange("instrumentId", instrumentId, 0xffff_ffffL);
        requireRange("size", size, 0xffff_ffffL);
        requireRange("channelId", channelId, 0xffL);
        requireRange("flags", flags, 0xffL);
        requireRange("sequence", sequence, 0xffff_ffffL);
        if (rtype != 160) {
            throw new IllegalArgumentException("MBO state machine requires rtype=160");
        }
        if (ACTIONS.indexOf(action) < 0) {
            throw new IllegalArgumentException("unsupported MBO action: " + action);
        }
        if (SIDES.indexOf(side) < 0) {
            throw new IllegalArgumentException("unsupported MBO side: " + side);
        }
    }

    private static void requireRange(String name, long value, long max) {
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(name + " outside unsigned range");
        }
    }

    private static void requireUnsigned(String name, long value) {
        // Java long 按位承载 UInt64；即使最高位为 1、数值显示为负数，也仍是合法的原始位模式。
    }
}
