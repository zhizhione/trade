package com.realtime.marketdata.replay.engine;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import com.realtime.marketdata.replay.model.ReplayBar;
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
    private static final int MAX_STREAM_FRAMES = 6_000;
    private static final MboBookEngineFactory ENGINE_FACTORY = new MboBookEngineFactory();
    private static final Logger log = LoggerFactory.getLogger(MboReplayStreamService.class);
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
        private int emittedFrames;

        private ReplayJob(
            String connectionId,
            ReplayStreamRequest request,
            BiConsumer<String, Object> sink
        ) {
            this.connectionId = connectionId;
            this.request = request;
            this.sink = sink;
            this.speed = request.speed();
        }

        @Override
        public void run() {
            try {
                stream();
            } catch (Exception exception) {
                if (!stopped) {
                    log.error("MBO replay stream failed for connection {}", connectionId, exception);
                    send("replay_error", Map.of("message", message(exception)));
                }
            } finally {
                jobs.remove(connectionId, this);
            }
        }

        private void stream() {
            long readStartedNanos = System.nanoTime();
            log.info(
                "Replay source read started: connectionId={}, publisherId={}, instrumentId={}, startMs={}, endMs={}",
                connectionId, request.publisherId(), request.instrumentId(), request.startMs(), request.endMs()
            );
            MboReplaySampler sampler = new MboReplaySampler(
                request.bucketMs(), ENGINE_FACTORY.create(false, request.depth()), request.startMs()
            );
            MutableBar bar = new MutableBar(request.barIntervalMs());
            long[] previousFrameMs = {-1L};
            boolean[] truncated = {false};
            List<BufferedMessage> buffered = new ArrayList<>();

            boolean completed = source.streamEvents(
                request.publisherId(),
                request.instrumentId(),
                request.startMs(),
                request.endMs(),
                event -> accept(
                    event, sampler, bar, previousFrameMs, truncated, buffered
                )
            );
            long readElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - readStartedNanos);
            if (stopped) {
                log.info(
                    "Replay source read stopped: connectionId={}, frames={}, elapsedMs={}",
                    connectionId, emittedFrames, readElapsedMs
                );
                return;
            }
            if (!completed && !truncated[0]) {
                log.warn(
                    "Replay source ended before completion: connectionId={}, frames={}, elapsedMs={}",
                    connectionId, emittedFrames, readElapsedMs
                );
                return;
            }
            if (completed && !truncated[0]) {
                bufferFrames(sampler.finish(), bar, previousFrameMs, truncated, buffered);
                if (!truncated[0]) {
                    bufferBar(buffered, bar.closeCompleted());
                }
            }
            buffered.add(new BufferedMessage("replay_complete", Map.of(
                "startMs", request.startMs(),
                "endMs", request.endMs(),
                "truncated", truncated[0]
            ), 0L));
            log.info(
                "Replay source read complete: connectionId={}, frames={}, messages={}, completed={}, truncated={}, elapsedMs={}",
                connectionId, emittedFrames, buffered.size(), completed, truncated[0], readElapsedMs
            );
            playBuffered(buffered);
        }

        private boolean accept(
            MboEvent event,
            MboReplaySampler sampler,
            MutableBar bar,
            long[] previousFrameMs,
            boolean[] truncated,
            List<BufferedMessage> buffered
        ) {
            if (stopped) {
                return false;
            }
            long timeMs = event.tsEventNs() / 1_000_000L;
            boolean withinRequestedEnd = timeMs <= request.endMs();
            if (!bufferFrames(
                sampler.accept(event), bar, previousFrameMs, truncated, buffered
            )) {
                return false;
            }
            return withinRequestedEnd;
        }

        private boolean bufferFrames(
            List<ReplayFrame> frames,
            MutableBar bar,
            long[] previousFrameMs,
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
                long previous = previousFrameMs[0];
                long delayMs = previous < 0
                    ? 0L
                    : Math.min(Math.max(0L, frame.timeMs() - previous), MAX_INTERACTIVE_GAP_MS);
                buffered.add(new BufferedMessage("replay_frame", frame, delayMs));
                emittedFrames += 1;
                previousFrameMs[0] = frame.timeMs();
                ReplayBar completedBar = bar.observe(frame);
                bufferBar(buffered, completedBar);
                bufferBar(buffered, bar.current());
                if (emittedFrames >= MAX_STREAM_FRAMES) {
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
                        connectionId, sentMessages, emittedFrames
                    );
                    return;
                }
                if (message.delayMs() > 0 && !awaitDelay(message.delayMs())) {
                    log.info(
                        "Replay playback stopped during delay: connectionId={}, messagesSent={}, frames={}",
                        connectionId, sentMessages, emittedFrames
                    );
                    return;
                }
                if (!awaitPlaying()) {
                    log.info(
                        "Replay playback stopped before message: connectionId={}, messagesSent={}, frames={}",
                        connectionId, sentMessages, emittedFrames
                    );
                    return;
                }
                send(message.type(), message.payload());
                sentMessages += 1;
            }
            log.info(
                "Replay playback complete: connectionId={}, messagesSent={}, frames={}, elapsedMs={}",
                connectionId, sentMessages, emittedFrames,
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

        private record BufferedMessage(String type, Object payload, long delayMs) {
        }
    }

    private static final class MutableBar {
        private final int intervalMs;
        private ReplayBar current;

        private MutableBar(int intervalMs) {
            this.intervalMs = intervalMs;
        }

        private ReplayBar observe(ReplayFrame frame) {
            if (!frame.complete() || frame.bids().isEmpty() || frame.asks().isEmpty() || frame.crossed()) {
                return null;
            }
            long midpoint = Math.floorDiv(
                Math.addExact(frame.bids().getFirst().priceNano(), frame.asks().getFirst().priceNano()),
                2
            );
            long bucket = Math.multiplyExact(Math.floorDiv(frame.timeMs(), intervalMs), intervalMs);
            ReplayBar completed = null;
            if (current == null || current.timeMs() != bucket) {
                completed = current;
                current = new ReplayBar(bucket, midpoint, midpoint, midpoint, midpoint);
            } else {
                current = new ReplayBar(
                    current.timeMs(),
                    current.openNano(),
                    Math.max(current.highNano(), midpoint),
                    Math.min(current.lowNano(), midpoint),
                    midpoint
                );
            }
            return completed;
        }

        private ReplayBar closeCompleted() {
            ReplayBar result = current;
            current = null;
            return result;
        }

        private ReplayBar current() {
            return current;
        }
    }
}
