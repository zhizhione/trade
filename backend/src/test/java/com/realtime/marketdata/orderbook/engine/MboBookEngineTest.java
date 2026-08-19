package com.realtime.marketdata.orderbook.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.realtime.marketdata.mbo.model.MboEvent;
import org.junit.jupiter.api.Test;

class MboBookEngineTest {

    @Test
    void emitsAnAggregatedSnapshotOnlyAtLastRecordBoundary() {
        MboBookEngine engine = new MboBookEngine();

        assertThat(engine.apply(event(0, 1, 'A', 'B', 20_000_000_000_000L, 4, 0))).isEmpty();
        assertThat(engine.apply(event(1, 2, 'A', 'B', 20_000_000_000_000L, 6, MboBookEngine.F_LAST)))
            .get()
            .satisfies(snapshot -> {
                assertThat(snapshot.bids()).containsExactly(
                    new MboBookEngine.Level(20_000_000_000_000L, 10, 2)
                );
                assertThat(snapshot.asks()).isEmpty();
                assertThat(snapshot.crossed()).isFalse();
                assertThat(snapshot.sourceOrdinal()).isEqualTo(1);
            });
    }

    @Test
    void retainsQueuePriorityOnSamePriceSizeReductionAndLosesItOnIncrease() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(0, 101, 'A', 'B', 100, 5, 0));
        engine.apply(event(1, 102, 'A', 'B', 100, 3, 0));

        engine.apply(event(2, 101, 'M', 'B', 100, 2, 0));
        assertThat(engine.ordersAtLevel(1, 750, 'B', 100))
            .extracting(MboBookEngine.OrderView::orderId)
            .containsExactly(101L, 102L);

        engine.apply(event(3, 101, 'M', 'B', 100, 7, 0));
        assertThat(engine.ordersAtLevel(1, 750, 'B', 100))
            .extracting(MboBookEngine.OrderView::orderId)
            .containsExactly(102L, 101L);
    }

    @Test
    void handlesPartialAndFullCancelsWithoutLeavingEmptyLevels() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(0, 101, 'A', 'B', 100, 10, 0));
        engine.apply(event(1, 101, 'C', 'B', 100, 4, 0));

        assertThat(engine.snapshot(1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(100, 6, 1));

        engine.apply(event(2, 101, 'C', 'B', 100, 6, 0));
        assertThat(engine.snapshot(1, 750, 10).bids()).isEmpty();
    }

    @Test
    void clearOnlyResetsItsOwnPublisherInstrumentBook() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(0, 101, 'A', 'B', 100, 1, 0));
        engine.apply(eventForInstrument(1, 751, 201, 'A', 'A', 110, 2, 0));
        engine.apply(event(2, 0, 'R', 'N', 0, 0, 0));

        assertThat(engine.snapshot(1, 750, 10).bids()).isEmpty();
        assertThat(engine.snapshot(1, 751, 10).asks())
            .containsExactly(new MboBookEngine.Level(110, 2, 1));
    }

    @Test
    void tradeFillAndNoopDoNotChangeRestingOrders() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(0, 101, 'A', 'B', 100, 8, 0));
        engine.apply(event(1, 0, 'T', 'N', 100, 3, 0));
        engine.apply(event(2, 0, 'F', 'N', 100, 3, 0));
        engine.apply(event(3, 0, 'N', 'N', 0, 0, MboBookEngine.F_LAST));

        assertThat(engine.snapshot(1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(100, 8, 1));
    }

    @Test
    void rejectsInvalidOrderLifecycleAndOutOfOrderEvents() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(1, 101, 'A', 'B', 100, 5, 0));

        assertThatThrownBy(() -> engine.apply(event(2, 101, 'C', 'B', 100, 6, 0)))
            .isInstanceOf(MboBookInvariantException.class)
            .hasMessageContaining("invalid Cancel size");
        assertThatThrownBy(() -> engine.apply(event(1, 102, 'A', 'B', 100, 1, 0)))
            .isInstanceOf(MboBookInvariantException.class)
            .hasMessageContaining("strictly increasing");
        assertThatThrownBy(() -> engine.apply(event(2, 999, 'M', 'B', 100, 1, 0)))
            .isInstanceOf(MboBookInvariantException.class)
            .hasMessageContaining("unknown order_id");
    }

    @Test
    void strictModeRejectsCrossedBookButAuditModeRetainsItsMarker() {
        MboBookEngine strict = new MboBookEngine();
        strict.apply(event(0, 101, 'A', 'B', 100, 1, 0));
        assertThatThrownBy(() -> strict.apply(event(1, 201, 'A', 'A', 100, 1, MboBookEngine.F_LAST)))
            .isInstanceOf(MboBookInvariantException.class)
            .hasMessageContaining("crossed book");

        MboBookEngine audit = new MboBookEngine(false);
        audit.apply(event(0, 101, 'A', 'B', 100, 1, 0));
        assertThat(audit.apply(event(1, 201, 'A', 'A', 100, 1, MboBookEngine.F_LAST)))
            .get()
            .extracting(MboBookEngine.BookSnapshot::crossed)
            .isEqualTo(true);
    }

    @Test
    void rejectedCrossingDoesNotMutateTheStrictBook() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(0, 101, 'A', 'B', 100, 1, MboBookEngine.F_LAST));

        assertThatThrownBy(() -> engine.apply(event(1, 201, 'A', 'A', 100, 1, MboBookEngine.F_LAST)))
            .isInstanceOf(MboBookInvariantException.class);

        assertThat(engine.snapshot(1, 750, 10).asks()).isEmpty();
        assertThat(engine.snapshot(1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(100, 1, 1));
    }

    @Test
    void rejectedMessageRollsBackAllOfItsHistoricalRecords() {
        MboBookEngine engine = new MboBookEngine();
        engine.apply(event(0, 101, 'A', 'B', 100, 1, MboBookEngine.F_LAST));

        assertThat(engine.apply(event(1, 201, 'A', 'A', 100, 1, 0))).isEmpty();
        assertThatThrownBy(() -> engine.apply(event(2, 0, 'N', 'N', 0, 0, MboBookEngine.F_LAST)))
            .isInstanceOf(MboBookInvariantException.class)
            .hasMessageContaining("crossed book");

        assertThat(engine.snapshot(1, 750, 10).asks()).isEmpty();
        assertThat(engine.snapshot(1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(100, 1, 1));
        assertThat(engine.apply(event(3, 202, 'A', 'A', 101, 1, MboBookEngine.F_LAST)))
            .get()
            .extracting(MboBookEngine.BookSnapshot::crossed)
            .isEqualTo(false);
    }

    @Test
    void rejectsDerivedRecordsAndNonMboRtypes() {
        assertThatThrownBy(() -> new MboEvent(0, 1, 1, 159, 1, 750, 'A', 'B', 100, 1, 0, 1, 0, 0, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rtype=160");

        MboBookEngine engine = new MboBookEngine();
        assertThatThrownBy(() -> engine.apply(event(0, 101, 'A', 'B', 100, 1, MboBookEngine.F_TOB)))
            .isInstanceOf(MboBookInvariantException.class)
            .hasMessageContaining("F_TOB/F_MBP");
    }

    private MboEvent event(long ordinal, long orderId, char action, char side, long price, long size, int flags) {
        return eventForInstrument(ordinal, 750, orderId, action, side, price, size, flags);
    }

    private MboEvent eventForInstrument(
        long ordinal,
        long instrumentId,
        long orderId,
        char action,
        char side,
        long price,
        long size,
        int flags
    ) {
        return new MboEvent(
            ordinal, 1_700_000_000_000_000_000L + ordinal,
            1_700_000_000_000_000_000L + ordinal, 160, 1, instrumentId,
            action, side, price, size, 0, orderId, flags, 0, ordinal
        );
    }
}
