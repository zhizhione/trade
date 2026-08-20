package com.realtime.marketdata.mbo.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.marketdata.mbo.model.LiveMboEvent;
import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import org.junit.jupiter.api.Test;

class MboStreamProcessorTest {

    @Test
    void usesTheSameBookTransitionsForHistoricalAndNormalizedLiveUpdates() {
        MboBookEngine historical = new MboBookEngine();
        historical.apply(historical(0, 10, 'A', 'B', 100, 8));
        historical.apply(historical(1, 10, 'M', 'B', 100, 5));
        historical.apply(historical(2, 10, 'C', 'B', 100, 2));

        MboStreamProcessor live = new MboStreamProcessor(false, 10);
        MboStreamKey stream = new MboStreamKey("atas", "connection-1");
        live.accept(stream, live(0, 10, LiveMboEvent.Action.ADD, 'B', 100, 8));
        live.accept(stream, live(1, 10, LiveMboEvent.Action.MODIFY, 'B', 100, 5));
        live.accept(stream, live(2, 10, LiveMboEvent.Action.CANCEL, 'B', 100, 2));

        assertThat(live.snapshot(stream, 1, 750, 10).bids())
            .isEqualTo(historical.snapshot(1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(100, 3, 1));
    }

    @Test
    void isolatesLiveConnectionsWhoseSequencesBothStartAtZero() {
        MboStreamProcessor live = new MboStreamProcessor(false, 10);
        MboStreamKey first = new MboStreamKey("atas", "connection-1");
        MboStreamKey second = new MboStreamKey("atas", "connection-2");

        live.accept(first, live(0, 11, LiveMboEvent.Action.ADD, 'B', 100, 4));
        live.accept(second, live(0, 12, LiveMboEvent.Action.ADD, 'B', 101, 6));

        assertThat(live.snapshot(first, 1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(100, 4, 1));
        assertThat(live.snapshot(second, 1, 750, 10).bids())
            .containsExactly(new MboBookEngine.Level(101, 6, 1));
    }

    @Test
    void removesAnAtasStyleDeleteWithoutInventingACancelQuantity() {
        MboStreamProcessor live = new MboStreamProcessor(false, 10);
        MboStreamKey stream = new MboStreamKey("atas", "connection-1");
        live.accept(stream, live(0, 10, LiveMboEvent.Action.ADD, 'A', 101, 7));
        live.accept(stream, live(1, 10, LiveMboEvent.Action.DELETE, 'A', 101, 0));

        assertThat(live.snapshot(stream, 1, 750, 10).asks()).isEmpty();
    }

    @Test
    void appliesExplicitAtasPriorityChangesEvenWhenPriceAndSizeStayTheSame() {
        MboStreamProcessor live = new MboStreamProcessor(false, 10);
        MboStreamKey stream = new MboStreamKey("atas", "priority-stream");
        live.accept(stream, live(0, 10, LiveMboEvent.Action.ADD, 'B', 100, 5, 10L));
        live.accept(stream, live(1, 11, LiveMboEvent.Action.ADD, 'B', 100, 5, 20L));
        live.accept(stream, live(2, 10, LiveMboEvent.Action.MODIFY, 'B', 100, 5, 30L));

        assertThat(live.ordersAtLevel(stream, 1, 750, 'B', 100))
            .extracting(MboBookEngine.OrderView::orderId)
            .containsExactly(11L, 10L);
    }

    private MboEvent historical(long ordinal, long orderId, char action, char side, long price, long size) {
        return new MboEvent(
            ordinal, 1_700_000_000_000_000_000L + ordinal,
            1_700_000_000_000_000_000L + ordinal, 160, 1, 750,
            action, side, price, size, 0, orderId, MboBookEngine.F_LAST, 0, ordinal
        );
    }

    private LiveMboEvent live(
        long ordinal,
        long orderId,
        LiveMboEvent.Action action,
        char side,
        long price,
        long size
    ) {
        return new LiveMboEvent(
            ordinal, 1_700_000_000_000_000_000L + ordinal,
            1_700_000_000_000_000_000L + ordinal, 1, 750,
            action, side, price, size, orderId, ordinal
        );
    }

    private LiveMboEvent live(
        long ordinal,
        long orderId,
        LiveMboEvent.Action action,
        char side,
        long price,
        long size,
        long priority
    ) {
        return new LiveMboEvent(
            ordinal, 1_700_000_000_000_000_000L + ordinal,
            1_700_000_000_000_000_000L + ordinal, 1, 750,
            action, side, price, size, orderId, ordinal, priority
        );
    }
}
