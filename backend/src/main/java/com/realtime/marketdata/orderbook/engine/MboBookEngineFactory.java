package com.realtime.marketdata.orderbook.engine;

/**
 * 按不同数据通路所需的策略创建订单簿引擎。
 *
 * <p>该工厂是状态机策略的唯一装配边界。无论回放通过 WebSocket 逐帧推送还是构建 REST
 * 会话，都必须使用完整盘口深度；实时通路仍保持严格的交叉盘校验。</p>
 */
public final class MboBookEngineFactory {
    /** 表示快照包含当前订单簿的全部价位，而非人为截断的档位数。 */
    public static final int UNBOUNDED_DEPTH = MboBookEngine.UNBOUNDED_DEPTH;
    public static final int HISTORICAL_DEPTH = UNBOUNDED_DEPTH;
    public static final int LIVE_DEPTH = UNBOUNDED_DEPTH;

    /** 创建历史回放引擎：允许保留交叉盘，并返回完整盘口。 */
    public MboBookEngine createHistorical() {
        return new MboBookEngine(false, HISTORICAL_DEPTH);
    }

    /** 创建实时引擎：严格拒绝交叉盘，并返回完整盘口。 */
    public MboBookEngine createLive() {
        return new MboBookEngine(true, LIVE_DEPTH);
    }

    /** 按调用方明确给出的交叉盘策略和深度创建引擎。 */
    public MboBookEngine create(boolean rejectCrossedBooks, int depth) {
        return new MboBookEngine(rejectCrossedBooks, depth);
    }
}
