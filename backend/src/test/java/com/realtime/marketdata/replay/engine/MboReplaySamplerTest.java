package com.realtime.marketdata.replay.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.orderbook.engine.MboBookInvariantException;
import com.realtime.marketdata.replay.model.ReplayFrame;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MboReplaySamplerTest {

    @Test
    void retainsOnlyTheLastEventSnapshotWithinEachBucket() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        sampler.accept(event(0, 10_000_000, 1, 'R', 'N', 0, 0, MboBookEngine.F_LAST));
        sampler.accept(event(1, 20_000_000, 2, 'A', 'B', 100, 4, MboBookEngine.F_LAST));
        List<ReplayFrame> emitted = sampler.accept(
            event(2, 120_000_000, 3, 'A', 'A', 101, 3, MboBookEngine.F_LAST)
        );

        assertThat(emitted).singleElement().satisfies(row -> {
            assertThat(row.timeMs()).isZero();
            assertThat(row.sourceOrdinal()).isEqualTo(1);
            assertThat(row.bids()).containsExactly(new ReplayFrame.DepthLevel(100, 4, 1));
            assertThat(row.addedSize()).isEqualTo(4);
            assertThat(row.cancelledSize()).isZero();
            assertThat(row.tradedSize()).isZero();
            assertThat(row.complete()).isTrue();
        });
        assertThat(sampler.finish()).singleElement().satisfies(row -> {
            assertThat(row.timeMs()).isEqualTo(100);
            assertThat(row.asks()).containsExactly(new ReplayFrame.DepthLevel(101, 3, 1));
        });
    }

    @Test
    void marksSnapshotsBeforeFirstClearAsIncomplete() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        sampler.accept(event(0, 0, 1, 'A', 'B', 100, 1, MboBookEngine.F_LAST));

        assertThat(sampler.finish()).singleElement().extracting(ReplayFrame::complete).isEqualTo(false);
    }

    @Test
    void marksTheBookCompleteWhenItObservesClearBeforeTheMessageBoundary() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        assertThat(sampler.accept(event(0, 0, 1, 'R', 'N', 0, 0, 0))).isEmpty();

        sampler.accept(event(1, 1, 2, 'A', 'B', 100, 1, MboBookEngine.F_LAST));

        assertThat(sampler.finish()).singleElement().extracting(ReplayFrame::complete).isEqualTo(true);
    }

    @Test
    void foldsEventTimeRegressionIntoTheLatestSourceOrderedBucket() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        sampler.accept(event(0, 200_000_000, 1, 'R', 'N', 0, 0, MboBookEngine.F_LAST));

        sampler.accept(event(1, 100_000_000, 2, 'N', 'N', 0, 0, MboBookEngine.F_LAST));

        assertThat(sampler.finish()).singleElement().satisfies(frame -> {
            assertThat(frame.timeMs()).isEqualTo(200L);
            assertThat(frame.sourceOrdinal()).isEqualTo(1L);
        });
    }

    @Test
    void doesNotRecreateAnAlreadyEmittedBucketWhenEventTimeRegresses() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        List<ReplayFrame> emitted = new ArrayList<>();
        emitted.addAll(sampler.accept(event(0, 0, 1, 'R', 'N', 0, 0, MboBookEngine.F_LAST)));
        emitted.addAll(sampler.accept(event(1, 100_000_000, 2, 'N', 'N', 0, 0, MboBookEngine.F_LAST)));
        emitted.addAll(sampler.accept(event(2, 0, 3, 'N', 'N', 0, 0, MboBookEngine.F_LAST)));
        emitted.addAll(sampler.finish());

        assertThat(emitted).extracting(ReplayFrame::timeMs).containsExactly(0L, 100L);
        assertThat(emitted).extracting(ReplayFrame::sourceOrdinal).containsExactly(0L, 2L);
    }

    @Test
    void ordersFinishedFramesUsingUnsignedSourceOrdinals() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        sampler.accept(eventForInstrument(
            Long.MAX_VALUE, 0, 750, 1, 'R', 'N', 0, 0, MboBookEngine.F_LAST
        ));
        sampler.accept(eventForInstrument(
            Long.MIN_VALUE, 0, 751, 2, 'R', 'N', 0, 0, MboBookEngine.F_LAST
        ));

        assertThat(sampler.finish()).extracting(ReplayFrame::sourceOrdinal)
            .containsExactly(Long.MAX_VALUE, Long.MIN_VALUE);
    }

    @Test
    void aggregatesAddCancelAndTradeFlowAcrossOneSamplingBucket() {
        MboReplaySampler sampler = new MboReplaySampler(100, 10, false);
        sampler.accept(event(0, 0, 1, 'R', 'N', 0, 0, MboBookEngine.F_LAST));
        sampler.accept(event(1, 10_000_000, 2, 'A', 'B', 100, 8, MboBookEngine.F_LAST));
        sampler.accept(event(2, 20_000_000, 2, 'C', 'B', 100, 3, MboBookEngine.F_LAST));
        sampler.accept(event(3, 30_000_000, 2, 'C', 'B', 100, 2, MboBookEngine.F_LAST));
        List<ReplayFrame> emitted = sampler.accept(
            event(4, 120_000_000, 3, 'T', 'N', 100, 5, MboBookEngine.F_LAST)
        );

        assertThat(emitted).singleElement().satisfies(row -> {
            assertThat(row.addedSize()).isEqualTo(8);
            assertThat(row.cancelledSize()).isEqualTo(5);
            assertThat(row.tradedSize()).isZero();
        });
        assertThat(sampler.finish()).singleElement().satisfies(row -> {
            assertThat(row.addedSize()).isZero();
            assertThat(row.cancelledSize()).isZero();
            assertThat(row.tradedSize()).isEqualTo(5);
        });
    }

    private MboEvent event(
        long ordinal, long tsEventNs, long orderId, char action, char side, long price, long size, int flags
    ) {
        return eventForInstrument(ordinal, tsEventNs, 750, orderId, action, side, price, size, flags);
    }

    private MboEvent eventForInstrument(
        long ordinal,
        long tsEventNs,
        long instrumentId,
        long orderId,
        char action,
        char side,
        long price,
        long size,
        int flags
    ) {
        return new MboEvent(
            ordinal, tsEventNs + 1, tsEventNs, 160, 1, instrumentId, action, side,
            price, size, 0, orderId, flags, 0, 0
        );
    }
}
