package com.realtime.marketdata.adapter.web.market;

import com.realtime.marketdata.core.event.MarketEvent;
import com.realtime.marketdata.market.port.MarketUpdatePublisher;
import com.realtime.marketdata.marketstate.model.MarketSnapshot;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * 向所有已连接客户端推送事件和快照。会话集合是并发安全的，但单个 WebSocket
 * session 的写入仍需串行化，防止多个 Kafka 消费线程交错写入同一帧序列。
 */
@Component
public class MarketWebSocketHandler extends TextWebSocketHandler implements MarketUpdatePublisher {

    private static final Logger log = LoggerFactory.getLogger(MarketWebSocketHandler.class);

    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
    private final ObjectMapper objectMapper;

    public MarketWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        sessions.add(session);
        // 连接确认不包含行情快照，客户端必须等待后续 snapshot 消息建立显示状态。
        session.sendMessage(new TextMessage(objectMapper.writeValueAsString(Map.of(
            "type", "status",
            "payload", Map.of("connected", true, "serverTime", Instant.now())
        ))));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        sessions.remove(session);
        log.debug("WebSocket transport error for session {}", session.getId(), exception);
    }

    @Override
    public void broadcastEvent(MarketEvent event) {
        broadcast("event", event);
    }

    @Override
    public void broadcastSnapshot(MarketSnapshot snapshot) {
        broadcast("snapshot", snapshot);
    }

    private void broadcast(String type, Object payload) {
        final String serialized;
        try {
            serialized = objectMapper.writeValueAsString(Map.of("type", type, "payload", payload));
        } catch (JacksonException exception) {
            log.error("Unable to serialize WebSocket message", exception);
            return;
        }

        TextMessage message = new TextMessage(serialized);
        // 先去除已关闭会话；发送失败也只影响该会话，不能阻塞其他订阅者。
        sessions.removeIf(session -> !session.isOpen());
        for (WebSocketSession session : sessions) {
            try {
                synchronized (session) {
                    session.sendMessage(message);
                }
            } catch (IOException exception) {
                sessions.remove(session);
                log.debug("Unable to push market data to session {}", session.getId(), exception);
            }
        }
    }
}
