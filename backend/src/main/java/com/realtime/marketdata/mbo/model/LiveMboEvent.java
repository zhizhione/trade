package com.realtime.marketdata.mbo.model;

/**
 * 一条已归一化的实时 L3（逐订单）更新。
 *
 * <p>历史 Databento 记录仍使用 {@link MboEvent} 保存其原生字段。实时供应商常采用不同的
 * 操作名称且没有 DBN 标志位，因此适配器须先转换为本记录，再交给 {@link MboBookEngine}。
 * 每条实时更新都视为一条完整消息，应用后可立即生成快照；同一来源流内的
 * {@code sourceOrdinal} 必须严格递增。</p>
 */
public record LiveMboEvent(
    long sourceOrdinal,
    long tsRecvNs,
    long tsEventNs,
    int publisherId,
    long instrumentId,
    Action action,
    char side,
    long priceNano,
    long size,
    long orderId,
    long sequence,
    Long priority
) {
    /** Sentinel used by delete updates when the provider omits side or price. */
    public static final long MISSING_PRICE_NANO = Long.MIN_VALUE;

    /** Provider queue priority when supplied by the live feed; null means derive it from source order. */
    public static final Long MISSING_PRIORITY = null;

    /** Backwards-compatible constructor for feeds without an explicit priority field. */
    public LiveMboEvent(
        long sourceOrdinal,
        long tsRecvNs,
        long tsEventNs,
        int publisherId,
        long instrumentId,
        Action action,
        char side,
        long priceNano,
        long size,
        long orderId,
        long sequence
    ) {
        this(sourceOrdinal, tsRecvNs, tsEventNs, publisherId, instrumentId, action, side,
            priceNano, size, orderId, sequence, MISSING_PRIORITY);
    }
    /**
     * {@code DELETE} 与 {@code CANCEL} 被有意区分：ATAS 等供应商可以发送“删除订单”而
     * 不给出本次撤销量。对于 DELETE，引擎会直接移除该活跃订单的全部剩余数量。
     */
    public enum Action {
        ADD('A'),
        MODIFY('M'),
        CANCEL('C'),
        DELETE('C'),
        CLEAR('R'),
        TRADE('T'),
        FILL('F'),
        NOOP('N');

        private final char snapshotAction;

        Action(char snapshotAction) {
            this.snapshotAction = snapshotAction;
        }

        public char snapshotAction() {
            return snapshotAction;
        }
    }

    public LiveMboEvent {
        if (publisherId < 0 || publisherId > 0xffff) {
            throw new IllegalArgumentException("publisherId outside UInt16");
        }
        if (action == null) {
            throw new IllegalArgumentException("live MBO action is required");
        }
        if (side != 'B' && side != 'A' && side != 'N') {
            throw new IllegalArgumentException("live MBO side must be B, A or N");
        }
        if (size < 0) {
            throw new IllegalArgumentException("live MBO size must be non-negative");
        }
    }
}
