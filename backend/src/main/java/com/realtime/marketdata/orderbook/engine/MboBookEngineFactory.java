package com.realtime.marketdata.orderbook.engine;

/**
 * 按不同数据通路所需的策略创建订单簿引擎。
 *
 * <p>该工厂是状态机策略的唯一装配边界。无论回放通过 WebSocket 逐帧推送还是实时通路，
 * 都统一限制为 400 档并保留 crossed 标记；需要严格拒绝交叉盘时由审计调用方显式创建。</p>
 */
public final class MboBookEngineFactory {
    public static final int MAX_DEPTH = MboBookEngine.MAX_DEPTH;
    public static final int HISTORICAL_DEPTH = MAX_DEPTH;
    public static final int LIVE_DEPTH = MAX_DEPTH;

    /** 创建历史回放引擎：按源记录逐条应用，允许保留交叉盘，并返回最多 400 档。 */
    public MboBookEngine createHistorical() {
        return new MboBookEngine(false, HISTORICAL_DEPTH);
    }

    /** 创建实时引擎：保留 crossed 标记，与历史引擎使用相同的异常盘策略，返回最多 400 档。 */
    public MboBookEngine createLive() {
        return new MboBookEngine(false, LIVE_DEPTH);
    }

    /** 按调用方明确给出的交叉盘策略和深度创建引擎。 */
    public MboBookEngine create(boolean rejectCrossedBooks, int depth) {
        return new MboBookEngine(rejectCrossedBooks, depth);
    }
}
