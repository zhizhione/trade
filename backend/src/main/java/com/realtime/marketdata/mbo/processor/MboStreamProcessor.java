package com.realtime.marketdata.mbo.processor;

import com.realtime.marketdata.mbo.model.LiveMboEvent;
import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * 将历史流和实时流路由到彼此隔离、但使用同一套 L3 规则的订单簿引擎实例。
 *
 * <p>{@link MboBookEngine} 要求源顺序号严格单调递增。以 {@link MboStreamKey} 为单位保留
 * 引擎，可同时满足 DBN 回放顺序和实时连接重连后序号重置的约束，避免不同流相互污染状态。</p>
 */
public final class MboStreamProcessor {
    private final ConcurrentMap<MboStreamKey, MboBookEngine> engines = new ConcurrentHashMap<>();
    private final Supplier<MboBookEngine> engineFactory;

    public MboStreamProcessor(boolean rejectCrossedBooks, int snapshotDepth) {
        if (snapshotDepth < 1) {
            throw new IllegalArgumentException("snapshotDepth must be positive");
        }
        this.engineFactory = () -> new MboBookEngine(rejectCrossedBooks, snapshotDepth);
    }

    /** 为每条隔离流创建标准的严格实时订单簿引擎。 */
    public MboStreamProcessor(MboBookEngineFactory factory) {
        this.engineFactory = factory::createLive;
    }

    public Optional<MboBookEngine.BookSnapshot> accept(MboStreamKey stream, MboEvent event) {
        MboBookEngine engine = engine(stream);
        synchronized (engine) {
            return engine.apply(event);
        }
    }

    public Optional<MboBookEngine.BookSnapshot> accept(MboStreamKey stream, LiveMboEvent event) {
        MboBookEngine engine = engine(stream);
        synchronized (engine) {
            return engine.apply(event);
        }
    }

    public MboBookEngine.BookSnapshot snapshot(
        MboStreamKey stream,
        int publisherId,
        long instrumentId,
        int depth
    ) {
        MboBookEngine engine = engine(stream);
        synchronized (engine) {
            return engine.snapshot(publisherId, instrumentId, depth);
        }
    }

    public List<MboBookEngine.OrderView> ordersAtLevel(
        MboStreamKey stream,
        int publisherId,
        long instrumentId,
        char side,
        long priceNano
    ) {
        MboBookEngine engine = engine(stream);
        synchronized (engine) {
            return engine.ordersAtLevel(publisherId, instrumentId, side, priceNano);
        }
    }

    /**
     * 仅丢弃一条已断开的实时来源流；历史回放流通常应持续保留至本次重建完成，不能因其他
     * 连接断开而被清除。
     */
    public void close(MboStreamKey stream) {
        engines.remove(stream);
    }

    private MboBookEngine engine(MboStreamKey stream) {
        return engines.computeIfAbsent(
            stream,
            ignored -> engineFactory.get()
        );
    }
}
