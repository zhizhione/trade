package com.realtime.marketdata.replay.engine;

import com.realtime.marketdata.mbo.model.MboEvent;
import com.realtime.marketdata.orderbook.engine.MboBookEngine;
import com.realtime.marketdata.orderbook.engine.MboBookEngineFactory;
import com.realtime.marketdata.replay.model.ReplayBar;
import com.realtime.marketdata.replay.model.ReplayFrame;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import com.realtime.marketdata.replay.source.MboReplayEventSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

/**
 * 为每个 WebSocket 连接运行一条独立的历史原始事件回放流。
 *
 * <p>数据库读取器和历史 MBO 通路遵循与实时通路一致的事件顺序。任务不会预先构造巨大的
 * 响应，而是等待播放/暂停命令，逐条应用原始事件，并在每条 DBN 消息完整后推送到浏览器。</p>
 */
@Service
public final class MboReplayStreamService {
    private static final int MAX_STREAM_FRAMES = 6_000;
    private static final MboBookEngineFactory ENGINE_FACTORY = new MboBookEngineFactory();
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
        ReplayJob job = new ReplayJob(connectionId, request, sink);
        jobs.put(connectionId, job);
        sink.accept("replay_ready", Map.of(
            "publisherId", request.publisherId(),
            "instrumentId", request.instrumentId(),
            "symbol", source.symbol(request.publisherId(), request.instrumentId()),
            "bucketMs", request.bucketMs(),
            "barIntervalMs", request.barIntervalMs(),
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
                    send("replay_error", Map.of("message", message(exception)));
                }
            } finally {
                jobs.remove(connectionId, this);
            }
        }

        private void stream() {
            MboBookEngine engine = ENGINE_FACTORY.createHistorical();
            Flow flow = new Flow();
            MutableBar bar = new MutableBar(request.barIntervalMs());
            long[] previousFrameMs = {-1L};
            boolean[] initialized = {false};

            boolean completed = source.streamEvents(
                request.publisherId(),
                request.instrumentId(),
                request.startMs(),
                request.endMs(),
                event -> accept(
                    event, engine, flow, bar, previousFrameMs, initialized
                )
            );
            if (completed && !stopped) {
                emitBar(bar.closeCompleted());
                send("replay_complete", Map.of(
                    "startMs", request.startMs(),
                    "endMs", request.endMs()
                ));
            }
        }

        private boolean accept(
            MboEvent event,
            MboBookEngine engine,
            Flow flow,
            MutableBar bar,
            long[] previousFrameMs,
            boolean[] initialized
        ) {
            if (!awaitPlaying()) {
                return false;
            }
            if (event.action() == 'R') {
                initialized[0] = true;
            }
            Optional<MboBookEngine.BookSnapshot> result = engine.apply(event);
            if (result.isEmpty()) {
                return true;
            }

            long timeMs = event.tsEventNs() / 1_000_000L;
            if (timeMs < request.startMs()) {
                return true;
            }
            if (timeMs > request.endMs()) {
                return false;
            }
            // 起始时间之前的事件仅用于预热订单簿，不能增加首个可见帧的新增、撤单和成交统计。
            flow.record(event);
            ReplayFrame frame = frame(event, result.get(), flow, initialized[0], timeMs);
            long previous = previousFrameMs[0];
            if (previous >= 0) {
                long eventDelta = Math.max(0L, timeMs - previous);
                long interactiveDelta = Math.min(eventDelta, MAX_INTERACTIVE_GAP_MS);
                if (!awaitDelay(interactiveDelta)) {
                    return false;
                }
            }
            if (!awaitPlaying()) {
                return false;
            }
            send("replay_frame", frame);
            emittedFrames += 1;
            previousFrameMs[0] = timeMs;
            ReplayBar completedBar = bar.observe(timeMs, result.get());
            emitBar(completedBar);
            emitBar(bar.current());
            flow.reset();
            if (emittedFrames >= MAX_STREAM_FRAMES) {
                // 限制浏览器端累积的帧数量，避免过大回放耗尽前端内存；用户可缩小时间窗口继续查询。
                send("replay_complete", Map.of(
                    "startMs", request.startMs(),
                    "endMs", request.endMs(),
                    "truncated", true
                ));
                return false;
            }
            return !stopped;
        }

        private ReplayFrame frame(
            MboEvent event,
            MboBookEngine.BookSnapshot snapshot,
            Flow flow,
            boolean complete,
            long timeMs
        ) {
            return new ReplayFrame(
                timeMs,
                event.sourceOrdinal(),
                event.sequence(),
                request.bucketMs(),
                flow.addedSize,
                flow.cancelledSize,
                flow.tradedSize,
                levels(snapshot.bids()),
                levels(snapshot.asks()),
                complete,
                snapshot.crossed()
            );
        }

        private List<ReplayFrame.DepthLevel> levels(List<MboBookEngine.Level> levels) {
            return levels.stream()
                .map(level -> new ReplayFrame.DepthLevel(level.priceNano(), level.size(), level.orderCount()))
                .toList();
        }

        private void emitBar(ReplayBar value) {
            if (value != null) {
                send("replay_bar", value);
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
    }

    private static final class Flow {
        private long addedSize;
        private long cancelledSize;
        private long tradedSize;

        private void record(MboEvent event) {
            switch (event.action()) {
                case 'A' -> addedSize = Math.addExact(addedSize, event.size());
                case 'C' -> cancelledSize = Math.addExact(cancelledSize, event.size());
                case 'T' -> tradedSize = Math.addExact(tradedSize, event.size());
                default -> { }
            }
        }

        private void reset() {
            addedSize = 0;
            cancelledSize = 0;
            tradedSize = 0;
        }
    }

    private static final class MutableBar {
        private final int intervalMs;
        private ReplayBar current;

        private MutableBar(int intervalMs) {
            this.intervalMs = intervalMs;
        }

        private ReplayBar observe(long timeMs, MboBookEngine.BookSnapshot snapshot) {
            if (!snapshot.hasBbo() || snapshot.crossed()) return null;
            long midpoint = Math.floorDiv(
                Math.addExact(snapshot.bids().getFirst().priceNano(), snapshot.asks().getFirst().priceNano()),
                2
            );
            long bucket = Math.multiplyExact(Math.floorDiv(timeMs, intervalMs), intervalMs);
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
