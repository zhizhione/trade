package com.realtime.marketdata.replay.engine;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayCursor;
import com.realtime.marketdata.replay.model.ReplayFrame;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 为每个 WebSocket 连接运行一条独立的历史原始事件回放流。
 *
 * <p>数据库读取器和历史 MBO 通路遵循与实时通路一致的事件顺序。任务先在受限窗口内
 * 按 {@code bucketMs} 重建回放帧并释放数据库游标，再等待播放/暂停命令按倍速推送，
 * 避免交互等待占用 DBN 流。</p>
 */
@Service
public final class MboReplayStreamService {
    static final int MAX_STREAM_FRAMES = 20_000;
    private static final MboBookEngineFactory ENGINE_FACTORY = new MboBookEngineFactory();
    private static final Logger log = LoggerFactory.getLogger(MboReplayStreamService.class);
    /** A locally buffered tail has no raw-source cursor, but still uses the existing continuation protocol. */
    private static final String FINALIZED_TAIL_CURSOR_SHA =
        "0000000000000000000000000000000000000000000000000000000000000000";
    /** 夜盘休市或周末的长时间空档不能阻塞交互式回放，因此会被压缩。 */
    private static final long MAX_INTERACTIVE_GAP_MS = 1_000L;

    private final MboReplayEventSource source;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ConcurrentMap<String, ReplayJob> jobs = new ConcurrentHashMap<>();

    /** 注入按源顺序读取原始 MBO 的事件源。 */
    public MboReplayStreamService(MboReplayEventSource source) {
        this.source = source;
    }

    /** 为一个 WebSocket 连接创建任务，发送就绪消息后等待客户端发出播放命令。 */
    public void start(
        String connectionId,
        ReplayStreamRequest request,
        BiConsumer<String, Object> sink
    ) {
        stop(connectionId);
        log.info(
            "Replay job started: connectionId={}, publisherId={}, instrumentId={}, startMs={}, endMs={}, bucketMs={}, barIntervalMs={}, speed={}, depth={}",
            connectionId, request.publisherId(), request.instrumentId(), request.startMs(), request.endMs(),
            request.bucketMs(), request.barIntervalMs(), request.speed(), request.depth()
        );
        ReplayJob job = new ReplayJob(connectionId, request, sink);
        jobs.put(connectionId, job);
        sink.accept("replay_ready", Map.of(
            "publisherId", request.publisherId(),
            "instrumentId", request.instrumentId(),
            "symbol", source.symbol(request.publisherId(), request.instrumentId()),
            "bucketMs", request.bucketMs(),
            "barIntervalMs", request.barIntervalMs(),
            "depth", request.depth(),
            "diagnostic", request.diagnostic(),
            "startMs", request.startMs(),
            "endMs", request.endMs()
        ));
        executor.submit(job);
    }

    /** 让指定连接继续处理事件；连接不存在或任务已结束时返回 false。 */
    public boolean play(String connectionId) {
        ReplayJob job = jobs.get(connectionId);
        return job != null && job.play();
    }

    /** 请求当前分段之后的下一段；游标必须来自上一条 replay_complete 消息。 */
    public boolean continueReplay(String connectionId, ReplayCursor cursor) {
        ReplayJob job = jobs.get(connectionId);
        return job != null && job.continueFrom(cursor);
    }

    /** 暂停指定连接的事件推进，但保留当前订单簿和回放游标。 */
    public boolean pause(String connectionId) {
        ReplayJob job = jobs.get(connectionId);
        return job != null && job.pause();
    }

    /** 修改指定连接的播放倍速；倍速影响事件间等待时间，不改变事件顺序。 */
    public boolean speed(String connectionId, double value) {
        ReplayJob job = jobs.get(connectionId);
        return job != null && job.speed(value);
    }

    /** 停止并移除指定连接的任务，释放其控制锁和后台读取流程。 */
    public void stop(String connectionId) {
        ReplayJob job = jobs.remove(connectionId);
        if (job != null) {
            log.info("Replay job stopping: connectionId={}", connectionId);
            job.stop();
        }
    }

    @PreDestroy
    /** 应用关闭时停止所有回放任务并等待线程池退出。 */
    public void shutdown() {
        jobs.values().forEach(ReplayJob::stop);
        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private final class ReplayJob implements Runnable {
        private final String connectionId;
        private final ReplayStreamRequest request;
        private final BiConsumer<String, Object> sink;
        private final ReentrantLock controlLock = new ReentrantLock();
        private final Condition controlChanged = controlLock.newCondition();
        private volatile boolean stopped;
        private boolean playing;
        private double speed;
        private final MboReplaySampler sampler;
        private final ReplayBarAggregator bar;
        private long previousFrameMs = -1L;
        private long totalEmittedFrames;
        private boolean continueRequested;
        private ReplayCursor requestedCursor;
        private List<ReplayFrame> finalizedTail = List.of();
        private int finalizedTailIndex;

        private ReplayJob(
            String connectionId,
            ReplayStreamRequest request,
            BiConsumer<String, Object> sink
        ) {
            this.connectionId = connectionId;
            this.request = request;
            this.sink = sink;
            this.speed = request.speed();
            this.sampler = new MboReplaySampler(
                request.bucketMs(), ENGINE_FACTORY.create(false, request.depth()), request.startMs()
            );
            this.bar = new ReplayBarAggregator(request.barIntervalMs());
        }

        @Override
        public void run() {
            try {
                streamChunks();
            } catch (Exception exception) {
                if (!stopped) {
                    log.error("MBO replay stream failed for connection {}", connectionId, exception);
                    send("replay_error", Map.of("message", message(exception)));
                }
            } finally {
                jobs.remove(connectionId, this);
            }
        }

        private void streamChunks() {
            ReplayCursor cursor = null;
            while (!stopped) {
                ChunkResult chunk = readChunk(cursor);
                if (chunk == null) return;
                playBuffered(chunk.buffered());
                if (chunk.nextCursor() == null) return;
                cursor = awaitContinuation(chunk.nextCursor());
                if (cursor == null) return;
            }
        }

        private ChunkResult readChunk(ReplayCursor cursor) {
            if (isFinalizedTailCursor(cursor)) {
                return readFinalizedTailChunk();
            }
            long readStartedNanos = System.nanoTime();
            int[] chunkFrames = {0};
            boolean[] truncated = {false};
            List<BufferedMessage> buffered = new ArrayList<>();
            log.info(
                "Replay source chunk started: connectionId={}, cursor={}, totalFrames={}",
                connectionId, cursor, totalEmittedFrames
            );
            MboReplayEventSource.StreamResult result = source.streamEvents(
                request.publisherId(),
                request.instrumentId(),
                request.startMs(),
                request.endMs(),
                cursor,
                event -> accept(event, chunkFrames, truncated, buffered)
            );
            if (stopped) return null;
            if (!result.completed() && result.nextCursor() == null) {
                log.warn("Replay source ended without a continuation cursor: connectionId={}", connectionId);
                return null;
            }
            if (result.completed()) {
                bufferFinalizedFrames(sampler.finish(), chunkFrames, truncated, buffered);
                if (!truncated[0]) bufferBar(buffered, bar.finish());
            }
            ReplayCursor nextCursor = result.completed() && hasFinalizedTail()
                ? finalizedTailCursor()
                : result.completed() ? null : result.nextCursor();
            buffered.add(new BufferedMessage("replay_complete", Map.of(
                "startMs", request.startMs(),
                "endMs", request.endMs(),
                "truncated", truncated[0],
                "hasNext", nextCursor != null,
                "nextCursor", nextCursor == null ? Map.of() : nextCursor
            ), 0L));
            log.info(
                "Replay source chunk ready: connectionId={}, chunkFrames={}, messages={}, hasNext={}, elapsedMs={}",
                connectionId, chunkFrames[0], buffered.size(), nextCursor != null,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - readStartedNanos)
            );
            return new ChunkResult(buffered, nextCursor);
        }

        private ChunkResult readFinalizedTailChunk() {
            int[] chunkFrames = {0};
            boolean[] truncated = {false};
            List<BufferedMessage> buffered = new ArrayList<>();
            while (finalizedTailIndex < finalizedTail.size()) {
                boolean accepted = bufferFrames(
                    List.of(finalizedTail.get(finalizedTailIndex)), chunkFrames, truncated, buffered
                );
                finalizedTailIndex += 1;
                if (!accepted) {
                    if (!truncated[0]) {
                        finalizedTailIndex = finalizedTail.size();
                    }
                    break;
                }
            }
            ReplayCursor nextCursor = hasFinalizedTail() ? finalizedTailCursor() : null;
            if (nextCursor == null) {
                finalizedTail = List.of();
                finalizedTailIndex = 0;
                bufferBar(buffered, bar.finish());
            }
            buffered.add(new BufferedMessage("replay_complete", Map.of(
                "startMs", request.startMs(),
                "endMs", request.endMs(),
                "truncated", truncated[0],
                "hasNext", nextCursor != null,
                "nextCursor", nextCursor == null ? Map.of() : nextCursor
            ), 0L));
            return new ChunkResult(buffered, nextCursor);
        }

        private void bufferFinalizedFrames(
            List<ReplayFrame> frames,
            int[] chunkFrames,
            boolean[] truncated,
            List<BufferedMessage> buffered
        ) {
            finalizedTail = List.copyOf(frames);
            finalizedTailIndex = 0;
            while (finalizedTailIndex < finalizedTail.size()) {
                boolean accepted = bufferFrames(
                    List.of(finalizedTail.get(finalizedTailIndex)), chunkFrames, truncated, buffered
                );
                finalizedTailIndex += 1;
                if (!accepted) {
                    if (!truncated[0]) {
                        finalizedTailIndex = finalizedTail.size();
                    }
                    break;
                }
            }
        }

        private boolean hasFinalizedTail() {
            return finalizedTailIndex < finalizedTail.size();
        }

        private ReplayCursor finalizedTailCursor() {
            return new ReplayCursor(
                FINALIZED_TAIL_CURSOR_SHA,
                Integer.toString(finalizedTailIndex),
                Integer.toString(finalizedTail.size())
            );
        }

        private boolean isFinalizedTailCursor(ReplayCursor cursor) {
            return cursor != null && cursor.equals(finalizedTailCursor());
        }

        private boolean accept(
            MboEvent event,
            int[] chunkFrames,
            boolean[] truncated,
            List<BufferedMessage> buffered
        ) {
            if (stopped) {
                return false;
            }
            long timeMs = event.tsEventNs() / 1_000_000L;
            boolean withinRequestedEnd = timeMs <= request.endMs();
            if (!bufferFrames(
                sampler.accept(event), chunkFrames, truncated, buffered
            )) {
                return false;
            }
            return withinRequestedEnd;
        }

        private boolean bufferFrames(
            List<ReplayFrame> frames,
            int[] chunkFrames,
            boolean[] truncated,
            List<BufferedMessage> buffered
        ) {
            long displayStartMs = Math.multiplyExact(
                Math.floorDiv(request.startMs(), request.bucketMs()), request.bucketMs()
            );
            for (ReplayFrame frame : frames) {
                if (frame.timeMs() < displayStartMs) {
                    continue;
                }
                if (frame.timeMs() > request.endMs()) {
                    return false;
                }
                long previous = previousFrameMs;
                long delayMs = previous < 0
                    ? 0L
                    : Math.min(Math.max(0L, frame.timeMs() - previous), MAX_INTERACTIVE_GAP_MS);
                buffered.add(new BufferedMessage("replay_frame", frame, delayMs));
                totalEmittedFrames += 1;
                chunkFrames[0] += 1;
                previousFrameMs = frame.timeMs();
                ReplayBar completedBar = bar.observe(frame);
                bufferBar(buffered, completedBar);
                bufferBar(buffered, bar.current());
                if (chunkFrames[0] >= MAX_STREAM_FRAMES) {
                    // 限制浏览器端累积的帧数量，避免过大回放耗尽前端内存；用户可缩小时间窗口继续查询。
                    truncated[0] = true;
                    return false;
                }
            }
            return true;
        }

        /**
         * 先快速消费数据库结果，再按播放控制发送帧。若在数据库 ResultSet 上等待倍速，
         * ClickHouse JDBC 的响应管道会在长回放中触发读超时并中断原始事件流。
         */
        private void playBuffered(List<BufferedMessage> buffered) {
            long playbackStartedNanos = System.nanoTime();
            int sentMessages = 0;
            for (BufferedMessage message : buffered) {
                if (!awaitPlaying()) {
                    log.info(
                        "Replay playback stopped before send: connectionId={}, messagesSent={}, frames={}",
                        connectionId, sentMessages, totalEmittedFrames
                    );
                    return;
                }
                if (message.delayMs() > 0 && !awaitDelay(message.delayMs())) {
                    log.info(
                        "Replay playback stopped during delay: connectionId={}, messagesSent={}, frames={}",
                        connectionId, sentMessages, totalEmittedFrames
                    );
                    return;
                }
                if (!awaitPlaying()) {
                    log.info(
                        "Replay playback stopped before message: connectionId={}, messagesSent={}, frames={}",
                        connectionId, sentMessages, totalEmittedFrames
                    );
                    return;
                }
                send(message.type(), message.payload());
                sentMessages += 1;
            }
            log.info(
                "Replay chunk playback complete: connectionId={}, messagesSent={}, frames={}, elapsedMs={}",
                connectionId, sentMessages, totalEmittedFrames,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - playbackStartedNanos)
            );
        }

        private void bufferBar(List<BufferedMessage> buffered, ReplayBar value) {
            if (value != null) {
                buffered.add(new BufferedMessage("replay_bar", value, 0L));
            }
        }

        private boolean awaitPlaying() {
            controlLock.lock();
            try {
                while (!playing && !stopped) {
                    controlChanged.await();
                }
                return !stopped;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                controlLock.unlock();
            }
        }

        private boolean awaitDelay(long eventDeltaMs) {
            long remainingMs = eventDeltaMs;
            while (remainingMs > 0 && !stopped) {
                controlLock.lock();
                try {
                    while (!playing && !stopped) {
                        controlChanged.await();
                    }
                    if (stopped) {
                        return false;
                    }
                    double activeSpeed = speed;
                    long waitMs = Math.max(1L, Math.round(remainingMs / activeSpeed));
                    long started = System.nanoTime();
                    controlChanged.await(waitMs, TimeUnit.MILLISECONDS);
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                    long replayElapsed = Math.round(elapsed * activeSpeed);
                    remainingMs = Math.max(0L, remainingMs - replayElapsed);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                } finally {
                    controlLock.unlock();
                }
            }
            return !stopped;
        }

        private ReplayCursor awaitContinuation(ReplayCursor expected) {
            controlLock.lock();
            try {
                while (!continueRequested && !stopped) {
                    controlChanged.await();
                }
                if (stopped) return null;
                ReplayCursor requested = requestedCursor;
                continueRequested = false;
                requestedCursor = null;
                return expected.equals(requested) ? requested : null;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return null;
            } finally {
                controlLock.unlock();
            }
        }

        private boolean play() {
            controlLock.lock();
            try {
                if (stopped) return false;
                playing = true;
                controlChanged.signalAll();
                return true;
            } finally {
                controlLock.unlock();
            }
        }

        private boolean pause() {
            controlLock.lock();
            try {
                if (stopped) return false;
                playing = false;
                controlChanged.signalAll();
                return true;
            } finally {
                controlLock.unlock();
            }
        }

        private boolean continueFrom(ReplayCursor cursor) {
            if (cursor == null) return false;
            controlLock.lock();
            try {
                if (stopped) return false;
                requestedCursor = cursor;
                continueRequested = true;
                controlChanged.signalAll();
                return true;
            } finally {
                controlLock.unlock();
            }
        }

        private boolean speed(double value) {
            if (!Double.isFinite(value) || value <= 0 || value > 1_000) return false;
            controlLock.lock();
            try {
                if (stopped) return false;
                speed = value;
                controlChanged.signalAll();
                return true;
            } finally {
                controlLock.unlock();
            }
        }

        private void stop() {
            stopped = true;
            controlLock.lock();
            try {
                controlChanged.signalAll();
            } finally {
                controlLock.unlock();
            }
        }

        private void send(String type, Object payload) {
            if (!stopped) {
                sink.accept(type, payload);
            }
        }

        private String message(Exception exception) {
            String message = exception.getMessage();
            return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
        }

        private record ChunkResult(List<BufferedMessage> buffered, ReplayCursor nextCursor) {
        }

        private record BufferedMessage(String type, Object payload, long delayMs) {
        }
    }

}
