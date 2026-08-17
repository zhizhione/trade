package com.realtime.marketdata.replay.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplayFrame;
import com.realtime.marketdata.replay.model.ReplaySession;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import java.util.List;
import org.junit.jupiter.api.Test;

class MboReplayServiceTest {
    @Test
    void buildsMidpointBarsWithoutUsingCrossedOrIncompleteBboFrames() {
        List<ReplayFrame> frames = List.of(
            frame(10, 100, 102, true, false),
            frame(500, 101, 103, true, false),
            frame(700, 90, 100, false, false),
            frame(900, 200, 199, true, true),
            frame(1_100, 102, 104, true, false)
        );

        assertThat(MboReplayService.midpointBars(frames, 1_000)).containsExactly(
            new ReplayBar(0, 101, 102, 101, 102),
            new ReplayBar(1_000, 103, 103, 103, 103)
        );
    }

    @Test
    void rebuildsEachRequestedWindowFromOrderedRawEvents() {
        InMemoryRawSource source = new InMemoryRawSource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 20_000_000, 201, 'A', 'A', 101, 7),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0),
            event(4, 220_000_000, 0, 'N', 'N', 0, 0)
        ));
        MboReplayService service = new MboReplayService(source);

        ReplaySession first = service.session(1, 750, 100, 0, 300, 1, 1_000);
        ReplaySession second = service.session(1, 750, 100, 100, 300, 1, 1_000);

        assertThat(first.frames()).singleElement().satisfies(frame -> {
            assertThat(frame.timeMs()).isZero();
            assertThat(frame.bids()).containsExactly(new ReplayFrame.DepthLevel(100, 10, 1));
            assertThat(frame.asks()).containsExactly(new ReplayFrame.DepthLevel(101, 7, 1));
            assertThat(frame.complete()).isTrue();
        });
        assertThat(first.nextStartMs()).isEqualTo(100);

        // 100 毫秒帧本身不含 Add 事件；其盘口深度证明请求先重放了更早的原始 Clear/Add 序列，
        // 再从所请求窗口输出可见帧。
        assertThat(second.frames()).singleElement().satisfies(frame -> {
            assertThat(frame.timeMs()).isEqualTo(100);
            assertThat(frame.bids()).containsExactly(new ReplayFrame.DepthLevel(100, 10, 1));
            assertThat(frame.asks()).containsExactly(new ReplayFrame.DepthLevel(101, 7, 1));
            assertThat(frame.addedSize()).isZero();
        });
        assertThat(second.nextStartMs()).isEqualTo(200);
        assertThat(source.streamCalls).isEqualTo(2);
    }

    @Test
    void limitsFramesToRequestedEndTimeAndStopsPagingAtTheEnd() {
        InMemoryRawSource source = new InMemoryRawSource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 20_000_000, 201, 'A', 'A', 101, 7),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0),
            event(4, 220_000_000, 0, 'N', 'N', 0, 0)
        ));

        ReplaySession bounded = new MboReplayService(source)
            .session(1, 750, 100, 100, 100, 10, 1_000);

        assertThat(bounded.frames()).extracting(ReplayFrame::timeMs).containsExactly(100L);
        assertThat(bounded.nextStartMs()).isNull();
    }

    @Test
    void keepsTransientCrossedSnapshotInHistoricalSession() {
        InMemoryRawSource source = new InMemoryRawSource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 20_000_000, 201, 'A', 'A', 99, 7),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0)
        ));

        ReplaySession session = new MboReplayService(source)
            .session(1, 750, 100, 0, 120, 10, 1_000);

        // HTTP 回放每个 100 毫秒时间桶只输出一帧最终的 F_LAST 快照。
        assertThat(session.frames()).extracting(ReplayFrame::timeMs)
            .containsExactly(0L, 100L);
        assertThat(session.frames()).allSatisfy(frame -> assertThat(frame.crossed()).isTrue());
        assertThat(session.bars()).isEmpty();
    }

    @Test
    void rejectsAnEndTimeBeforeTheStartTime() {
        MboReplayService service = new MboReplayService(new InMemoryRawSource(List.of()));

        assertThatThrownBy(() -> service.session(1, 750, 100, 101, 100, 10, 1_000))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid replay range");
    }

    @Test
    void doesNotCountWarmupEventsInTheFirstVisibleFrame() {
        InMemoryRawSource source = new InMemoryRawSource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 40_000_000, 101, 'A', 'B', 100, 10),
            event(2, 60_000_000, 101, 'C', 'B', 100, 3),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0)
        ));

        ReplaySession session = new MboReplayService(source)
            .session(1, 750, 100, 50, 120, 10, 1_000);

        assertThat(session.frames()).first().satisfies(frame -> {
            assertThat(frame.timeMs()).isEqualTo(0L);
            assertThat(frame.addedSize()).isZero();
            assertThat(frame.cancelledSize()).isEqualTo(3);
            assertThat(frame.bids()).containsExactly(new ReplayFrame.DepthLevel(100, 7, 1));
        });
    }

    private ReplayFrame frame(long timeMs, long bid, long ask, boolean complete, boolean crossed) {
        return new ReplayFrame(
            timeMs, timeMs, timeMs, 100,
            0, 0, 0,
            List.of(new ReplayFrame.DepthLevel(bid, 1, 1)),
            List.of(new ReplayFrame.DepthLevel(ask, 1, 1)),
            complete,
            crossed
        );
    }

    private MboEvent event(
        long ordinal,
        long tsEventNs,
        long orderId,
        char action,
        char side,
        long price,
        long size
    ) {
        return new MboEvent(
            ordinal,
            tsEventNs + 1,
            tsEventNs,
            160,
            1,
            750,
            action,
            side,
            price,
            size,
            0,
            orderId,
            MboBookEngine.F_LAST,
            0,
            ordinal
        );
    }

    private static final class InMemoryRawSource implements MboReplayEventSource {
        private final List<MboEvent> events;
        private int streamCalls;

        private InMemoryRawSource(List<MboEvent> events) {
            this.events = List.copyOf(events);
        }

        @Override
        public List<ReplayCatalogEntry> catalog() {
            return List.of();
        }

        @Override
        public boolean streamEvents(
            int publisherId,
            long instrumentId,
            long startMs,
            long endMs,
            MboEventConsumer consumer
        ) {
            streamCalls++;
            for (MboEvent event : events) {
                if (!consumer.accept(event)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String symbol(int publisherId, long instrumentId) {
            return "NQ";
        }
    }
}
