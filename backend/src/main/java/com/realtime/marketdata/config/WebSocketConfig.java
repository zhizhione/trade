package com.realtime.marketdata.config;

import com.realtime.marketdata.adapter.web.market.MarketWebSocketHandler;
import com.realtime.marketdata.adapter.web.replay.ReplayWebSocketHandler;
import java.util.Arrays;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** 将只读行情推送暴露在固定端点，并从配置解析允许的前端来源。 */
@EnableWebSocket
@Configuration
public class WebSocketConfig implements WebSocketConfigurer {

    private final MarketWebSocketHandler marketWebSocketHandler;
    private final ReplayWebSocketHandler replayWebSocketHandler;
    private final String[] allowedOrigins;

    public WebSocketConfig(
        MarketWebSocketHandler marketWebSocketHandler,
        ReplayWebSocketHandler replayWebSocketHandler,
        @Value("${app.websocket.allowed-origin-patterns:http://localhost:*,http://127.0.0.1:*}") String allowedOrigins
    ) {
        this.marketWebSocketHandler = marketWebSocketHandler;
        this.replayWebSocketHandler = replayWebSocketHandler;
        this.allowedOrigins = Arrays.stream(allowedOrigins.split(","))
            .map(String::trim)
            .filter(origin -> !origin.isEmpty())
            .toArray(String[]::new);
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // 使用 pattern 而非硬编码单一端口，便于本地 Vite 端口变更；生产环境应配置为明确域名。
        registry.addHandler(marketWebSocketHandler, "/ws/market")
            .setAllowedOriginPatterns(allowedOrigins);
        registry.addHandler(replayWebSocketHandler, "/ws/replay")
            .setAllowedOriginPatterns(allowedOrigins);
    }
}
