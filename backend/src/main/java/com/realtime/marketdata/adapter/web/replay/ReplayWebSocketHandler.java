package com.realtime.marketdata.adapter.web.replay;

import com.realtime.marketdata.replay.engine.MboReplayStreamService;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 服务端驱动的历史回放所使用的 WebSocket 命令与数据通道。它负责解析播放控制命令并将回放帧、
 * K 线和完成状态推送给对应连接，不承担订单簿重建或数据库读取职责。
 */
@Component
public final class ReplayWebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(ReplayWebSocketHandler.class);
    private final ObjectMapper objectMapper;
    private final MboReplayStreamService replay;

    public ReplayWebSocketHandler(ObjectMapper objectMapper, MboReplayStreamService replay) {
        this.objectMapper = objectMapper;
        this.replay = replay;
    }

    @Override
    /** 建立连接后仅确认通道可用，订单簿任务需由客户端随后发送 replay_start 创建。 */
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("Replay WebSocket connected: connectionId={}", session.getId());
        sendQuietly(session, "status", Map.of("connected", true, "serverTime", Instant.now().toString()));
    }

    @Override
    /** 连接关闭时同步停止后台回放，避免任务继续读取数据库并向失效会话写消息。 */
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.info("Replay WebSocket closed: connectionId={}, status={}", session.getId(), status);
        replay.stop(session.getId());
    }

    @Override
    /** 发生传输错误时与主动关闭采用相同清理策略。 */
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        logTransportFailure(session, "transport", exception);
        replay.stop(session.getId());
    }

    @Override
    /**
     * 解析 replay_start、replay_play、replay_pause、replay_speed 与 replay_stop 命令，
     * 所有响应统一封装为 {type, payload}，使前端可按消息类型处理。
     */
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            JsonNode root = objectMapper.readTree(message.getPayload());
            String type = text(root, "type");
            JsonNode payload = root.get("payload");
            log.debug("Replay command received: connectionId={}, type={}", session.getId(), type);
            switch (type) {
                case "replay_start" -> {
                    ReplayStreamRequest request = request(payload);
                    log.info(
                        "Replay start requested: connectionId={}, publisherId={}, instrumentId={}, startMs={}, endMs={}, speed={}, depth={}",
                        session.getId(), request.publisherId(), request.instrumentId(),
                        request.startMs(), request.endMs(), request.speed(), request.depth()
                    );
                    replay.start(
                        session.getId(),
                        request,
                        (eventType, value) -> sendQuietly(session, eventType, value)
                    );
                }
                case "replay_play" -> {
                    log.info("Replay play requested: connectionId={}", session.getId());
                    control(session, "play", replay.play(session.getId()));
                }
                case "replay_pause" -> {
                    log.info("Replay pause requested: connectionId={}", session.getId());
                    control(session, "pause", replay.pause(session.getId()));
                }
                case "replay_speed" -> control(session, "speed", speed(session, payload));
                case "replay_stop" -> {
                    log.info("Replay stop requested: connectionId={}", session.getId());
                    replay.stop(session.getId());
                }
                default -> {
                    log.warn("Unknown replay command: connectionId={}, type={}", session.getId(), type);
                    send(session, "replay_error", Map.of("message", "未知回放命令: " + type));
                }
            }
        } catch (JacksonException | IllegalArgumentException | IllegalStateException exception) {
            log.warn("Invalid replay command: connectionId={}", session.getId(), exception);
            sendQuietly(session, "replay_error", Map.of("message", message(exception)));
        }
    }

    private ReplayStreamRequest request(JsonNode payload) {
        if (payload == null) throw new IllegalArgumentException("回放参数不能为空");
        return new ReplayStreamRequest(
            (int) number(payload, "publisherId"),
            number(payload, "instrumentId"),
            (int) optionalNumber(payload, "bucketMs", 100),
            number(payload, "startMs"),
            number(payload, "endMs"),
            (int) optionalNumber(payload, "barIntervalMs", 1000),
            optionalDecimal(payload, "speed", 1.0),
            optionalBoolean(payload, "diagnostic", false)
        );
    }

    private long number(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) throw new IllegalArgumentException("缺少回放参数: " + field);
        return value.asLong();
    }

    private long optionalNumber(JsonNode node, String field, long fallback) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? fallback : value.asLong();
    }

    private double optionalDecimal(JsonNode node, String field, double fallback) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? fallback : value.asDouble();
    }

    private boolean optionalBoolean(JsonNode node, String field, boolean fallback) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? fallback : value.asBoolean();
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? "" : value.asString();
    }

    private void control(WebSocketSession session, String operation, boolean accepted) {
        if (!accepted) {
            // 播放完成、切换查询或连接清理后，客户端可能还有已排队的控制命令；这是正常竞态。
            log.debug(
                "Replay control ignored because no active job exists: connectionId={}, operation={}",
                session.getId(), operation
            );
        }
    }

    private boolean speed(WebSocketSession session, JsonNode payload) {
        JsonNode valueNode = payload == null ? null : payload.get("speed");
        if (valueNode == null || valueNode.isNull()) {
            throw new IllegalArgumentException("缺少回放参数: speed");
        }
        double value = valueNode.asDouble();
        log.info("Replay speed requested: connectionId={}, speed={}", session.getId(), value);
        return replay.speed(session.getId(), value);
    }

    private void sendQuietly(WebSocketSession session, String type, Object payload) {
        try {
            send(session, type, payload);
        } catch (RuntimeException exception) {
            logTransportFailure(session, type, exception);
            replay.stop(session.getId());
        }
    }

    private void logTransportFailure(WebSocketSession session, String operation, Throwable exception) {
        if (isClientDisconnect(exception)) {
            log.debug(
                "Replay WebSocket client disconnected: connectionId={}, operation={}, cause={}",
                session.getId(), operation, rootCauseMessage(exception)
            );
            return;
        }
        log.warn(
            "Unable to send replay WebSocket message: connectionId={}, operation={}",
            session.getId(), operation, exception
        );
    }

    private boolean isClientDisconnect(Throwable exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof EOFException || cause instanceof ClosedChannelException) {
                return true;
            }
            String detail = cause.getMessage();
            if (detail == null) continue;
            String normalized = detail.toLowerCase(Locale.ROOT);
            if (normalized.contains("broken pipe")
                || normalized.contains("connection reset")
                || normalized.contains("connection aborted")
                || normalized.contains("socket closed")) {
                return true;
            }
        }
        return false;
    }

    private String rootCauseMessage(Throwable exception) {
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String detail = cause.getMessage();
        return detail == null || detail.isBlank() ? cause.getClass().getSimpleName() : detail;
    }

    private void send(WebSocketSession session, String type, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(Map.of("type", type, "payload", payload));
            synchronized (session) {
                if (session.isOpen()) session.sendMessage(new TextMessage(json));
            }
        } catch (JacksonException | IOException exception) {
            throw new IllegalStateException("无法发送回放消息", exception);
        }
    }

    private String message(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }
}
