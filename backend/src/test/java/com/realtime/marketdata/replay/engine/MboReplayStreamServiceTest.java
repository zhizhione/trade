package com.realtime.marketdata.replay.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplayFrame;
import com.realtime.marketdata.replay.model.ReplayCursor;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    void streamsBucketedSnapshotsOnlyAfterPlayAndBuildsBarsAtTheRequestedInterval() throws Exception {
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

        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 100L);
        assertThat(frames.getFirst()).satisfies(frame -> {
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
    void readsTheSourceBeforeWaitingForPlayback() throws Exception {
        CountDownLatch sourceFinished = new CountDownLatch(1);
        InMemorySource source = new InMemorySource(List.of(
            event(0, 0, 0, 'R', 'N', 0, 0),
            event(1, 10_000_000, 101, 'A', 'B', 100, 10),
            event(2, 20_000_000, 201, 'A', 'A', 102, 7)
        ), sourceFinished);
        service = new MboReplayStreamService(source);
        List<ReplayFrame> frames = new ArrayList<>();

        service.start("socket-buffered", new ReplayStreamRequest(1, 750, 100, 0, 20, 100, 1_000),
            (type, payload) -> {
                if ("replay_frame".equals(type)) frames.add((ReplayFrame) payload);
            }
        );

        assertThat(sourceFinished.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(frames).isEmpty();
        assertThat(service.play("socket-buffered")).isTrue();
        // The source has already been fully consumed, so playback only waits on the local buffer.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (frames.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(frames).hasSize(1);
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
        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 10_000L);
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
        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 100L);
        assertThat(frames.getFirst().crossed()).isTrue();
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
        assertThat(frames).extracting(ReplayFrame::timeMs).containsExactly(0L, 100L);
        assertThat(frames.getFirst()).satisfies(frame -> {
            assertThat(frame.addedSize()).isZero();
            assertThat(frame.cancelledSize()).isEqualTo(3);
        });
    }

    @Test
    void continuesLocallyBufferedFinalFramesInsteadOfDroppingTheTail() throws Exception {
        List<MboEvent> events = new ArrayList<>(MboReplayStreamService.MAX_STREAM_FRAMES + 1);
        for (int index = 0; index <= MboReplayStreamService.MAX_STREAM_FRAMES; index++) {
            events.add(eventForInstrument(index, 0, 750L + index, 0, 'R', 'N', 0, 0));
        }
        service = new MboReplayStreamService(new InMemorySource(events));
        List<ReplayFrame> frames = Collections.synchronizedList(new ArrayList<>());
        List<ReplayCursor> cursors = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch firstChunk = new CountDownLatch(1);
        CountDownLatch complete = new CountDownLatch(1);

        service.start("socket-final-tail", new ReplayStreamRequest(1, 750, 100, 0, 0, 100, 1_000),
            (type, payload) -> {
                if ("replay_frame".equals(type)) frames.add((ReplayFrame) payload);
                if ("replay_complete".equals(type)) {
                    Map<?, ?> message = (Map<?, ?>) payload;
                    if (Boolean.TRUE.equals(message.get("hasNext"))) {
                        cursors.add((ReplayCursor) message.get("nextCursor"));
                        firstChunk.countDown();
                    } else {
                        complete.countDown();
                    }
                }
            }
        );

        assertThat(service.play("socket-final-tail")).isTrue();
        assertThat(firstChunk.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(frames).hasSize(MboReplayStreamService.MAX_STREAM_FRAMES);
        assertThat(cursors).singleElement().satisfies(cursor ->
            assertThat(service.continueReplay("socket-final-tail", cursor)).isTrue()
        );
        assertThat(complete.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(frames).hasSize(MboReplayStreamService.MAX_STREAM_FRAMES + 1);
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
        return eventForInstrument(ordinal, tsEventNs, 750, orderId, action, side, price, size);
    }

    private MboEvent eventForInstrument(
        long ordinal,
        long tsEventNs,
        long instrumentId,
        long orderId,
        char action,
        char side,
        long price,
        long size
    ) {
        return new MboEvent(
            ordinal, tsEventNs + 1, tsEventNs, 160, 1, instrumentId, action, side,
            price, size, 0, orderId, MboBookEngine.F_LAST, 0, ordinal
        );
    }

    private static final class InMemorySource implements MboReplayEventSource {
        private final List<MboEvent> events;
        private final CountDownLatch finished;

        private InMemorySource(List<MboEvent> events) {
            this(events, new CountDownLatch(0));
        }

        private InMemorySource(List<MboEvent> events, CountDownLatch finished) {
            this.events = List.copyOf(events);
            this.finished = finished;
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
            try {
                for (MboEvent event : events) {
                    if (!consumer.accept(event)) return false;
                }
                return true;
            } finally {
                finished.countDown();
            }
        }

        @Override
        public String symbol(int publisherId, long instrumentId) {
            return "NQ";
        }
    }
}
