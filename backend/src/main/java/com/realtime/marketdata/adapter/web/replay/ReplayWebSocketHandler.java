package com.realtime.marketdata.adapter.web.replay;

import com.realtime.marketdata.replay.engine.MboReplayStreamService;
import com.realtime.marketdata.replay.model.ReplayStreamRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 服务端驱动的历史回放所使用的 WebSocket 命令与数据通道。它负责解析播放控制命令并将回放帧、
 * K 线和完成状态推送给对应连接，不承担订单簿重建或数据库读取职责。
 */
@Component
public final class ReplayWebSocketHandler extends TextWebSocketHandler {
    private final ObjectMapper objectMapper;
    private final MboReplayStreamService replay;

    public ReplayWebSocketHandler(ObjectMapper objectMapper, MboReplayStreamService replay) {
        this.objectMapper = objectMapper;
        this.replay = replay;
    }

    @Override
    /** 建立连接后仅确认通道可用，订单簿任务需由客户端随后发送 replay_start 创建。 */
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        sendQuietly(session, "status", Map.of("connected", true, "serverTime", Instant.now().toString()));
    }

    @Override
    /** 连接关闭时同步停止后台回放，避免任务继续读取数据库并向失效会话写消息。 */
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        replay.stop(session.getId());
    }

    @Override
    /** 发生传输错误时与主动关闭采用相同清理策略。 */
    public void handleTransportError(WebSocketSession session, Throwable exception) {
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
            switch (type) {
                case "replay_start" -> replay.start(
                    session.getId(),
                    request(payload),
                    (eventType, value) -> sendQuietly(session, eventType, value)
                );
                case "replay_play" -> requireControl(replay.play(session.getId()), "回放任务不存在");
                case "replay_pause" -> requireControl(replay.pause(session.getId()), "回放任务不存在");
                case "replay_speed" -> requireControl(
                    replay.speed(session.getId(), number(payload, "speed")),
                    "回放任务不存在"
                );
                case "replay_stop" -> replay.stop(session.getId());
                default -> send(session, "replay_error", Map.of("message", "未知回放命令: " + type));
            }
        } catch (JacksonException | IllegalArgumentException | IllegalStateException exception) {
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
            optionalDecimal(payload, "speed", 1.0)
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

    private String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? "" : value.asString();
    }

    private void requireControl(boolean result, String message) {
        if (!result) throw new IllegalStateException(message);
    }

    private void sendQuietly(WebSocketSession session, String type, Object payload) {
        try {
            send(session, type, payload);
        } catch (RuntimeException ignored) {
            replay.stop(session.getId());
        }
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
