package com.realtime.marketdata.replay.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplayFrame;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MboReplayStreamServiceTest {
    private MboReplayStreamService service;

    @AfterEach
    void stopJobs() {
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    void streamsFLastSnapshotsOnlyAfterPlayAndBuildsBarsAtTheRequestedInterval() throws Exception {
        InMemorySource source = new InMemorySource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 20_000_000, 201, 'A', 'A', 102, 7),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0)
        ));
        service = new MboReplayStreamService(source);
        List<ReplayFrame> frames = new ArrayList<>();
        List<ReplayBar> bars = new ArrayList<>();
        CountDownLatch complete = new CountDownLatch(1);

        service.start("socket-1", new ReplayStreamRequest(1, 750, 100, 0, 120, 100, 1_000),
            (type, payload) -> {
                if ("replay_frame".equals(type)) frames.add((ReplayFrame) payload);
                if ("replay_bar".equals(type)) bars.add((ReplayBar) payload);
                if ("replay_complete".equals(type)) complete.countDown();
            }
        );

        Thread.sleep(30);
        assertThat(frames).isEmpty();

        assertThat(service.play("socket-1")).isTrue();
        assertThat(complete.await(2, TimeUnit.SECONDS)).isTrue();

        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 10L, 20L, 120L);
        assertThat(frames.get(2)).satisfies(frame -> {
            assertThat(frame.complete()).isTrue();
            assertThat(frame.bids()).containsExactly(new ReplayFrame.DepthLevel(100, 10, 1));
            assertThat(frame.asks()).containsExactly(new ReplayFrame.DepthLevel(102, 7, 1));
        });
        assertThat(bars).containsExactly(
            new ReplayBar(0, 101, 101, 101, 101),
            new ReplayBar(0, 101, 101, 101, 101),
            new ReplayBar(100, 101, 101, 101, 101),
            new ReplayBar(100, 101, 101, 101, 101)
        );
    }

    @Test
    void compressesLongHistoricalGapsForInteractivePlayback() throws Exception {
        InMemorySource source = new InMemorySource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 10_000_000_000L, 0, 'N', 'N', 0, 0)
        ));
        service = new MboReplayStreamService(source);
        List<ReplayFrame> frames = new ArrayList<>();
        CountDownLatch complete = new CountDownLatch(1);

        service.start("socket-gap", new ReplayStreamRequest(1, 750, 100, 0, 10_000, 100, 1),
            (type, payload) -> {
                if ("replay_frame".equals(type)) frames.add((ReplayFrame) payload);
                if ("replay_complete".equals(type)) complete.countDown();
            }
        );

        assertThat(service.play("socket-gap")).isTrue();
        assertThat(complete.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 10L, 10_000L);
    }

    @Test
    void continuesAfterTransientCrossedSnapshot() throws Exception {
        InMemorySource source = new InMemorySource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 20_000_000, 201, 'A', 'A', 99, 7),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0)
        ));
        service = new MboReplayStreamService(source);
        List<ReplayFrame> frames = new ArrayList<>();
        CountDownLatch complete = new CountDownLatch(1);

        service.start("socket-crossed", new ReplayStreamRequest(1, 750, 100, 0, 120, 100, 1_000),
            (type, payload) -> {
                if ("replay_frame".equals(type)) frames.add((ReplayFrame) payload);
                if ("replay_complete".equals(type)) complete.countDown();
            }
        );

        assertThat(service.play("socket-crossed")).isTrue();
        assertThat(complete.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 10L, 20L, 120L);
        assertThat(frames.get(2).crossed()).isTrue();
    }

    @Test
    void doesNotCountEventsBeforeTheRequestedStart() throws Exception {
        InMemorySource source = new InMemorySource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 40_000_000, 101, 'A', 'B', 100, 10),
            event(2, 60_000_000, 101, 'C', 'B', 100, 3),
            event(3, 120_000_000, 0, 'N', 'N', 0, 0)
        ));
        service = new MboReplayStreamService(source);
        List<ReplayFrame> frames = new ArrayList<>();
        CountDownLatch complete = new CountDownLatch(1);

        service.start("socket-window", new ReplayStreamRequest(1, 750, 100, 50, 120, 100, 1_000),
            (type, payload) -> {
                if ("replay_frame".equals(type)) frames.add((ReplayFrame) payload);
                if ("replay_complete".equals(type)) complete.countDown();
            }
        );

        assertThat(service.play("socket-window")).isTrue();
        assertThat(complete.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(60L, 120L);
        assertThat(frames.getFirst()).satisfies(frame -> {
            assertThat(frame.addedSize()).isZero();
            assertThat(frame.cancelledSize()).isEqualTo(3);
        });
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
            ordinal, tsEventNs + 1, tsEventNs, 160, 1, 750, action, side,
            price, size, 0, orderId, MboBookEngine.F_LAST, 0, ordinal
        );
    }

    private static final class InMemorySource implements MboReplayEventSource {
        private final List<MboEvent> events;

        private InMemorySource(List<MboEvent> events) {
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
            for (MboEvent event : events) {
                if (!consumer.accept(event)) return false;
            }
            return true;
        }

        @Override
        public String symbol(int publisherId, long instrumentId) {
            return "NQ";
        }
    }
}
