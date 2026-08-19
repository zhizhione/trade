package com.realtime.marketdata.adapter.web.replay;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.realtime.marketdata.replay.engine.MboReplayStreamService;
import com.realtime.marketdata.replay.model.ReplayCursor;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import tools.jackson.databind.json.JsonMapper;

class ReplayWebSocketHandlerTest {

    @Test
    void ignoresAClientDisconnectWhileSendingTheConnectionStatus() throws Exception {
        MboReplayStreamService replay = mock(MboReplayStreamService.class);
        ReplayWebSocketHandler handler = new ReplayWebSocketHandler(JsonMapper.builder().build(), replay);
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("connection-1");
        when(session.isOpen()).thenReturn(true);
        doThrow(new IOException("Broken pipe")).when(session).sendMessage(any(TextMessage.class));

        assertThatCode(() -> handler.afterConnectionEstablished(session)).doesNotThrowAnyException();

        verify(replay).stop("connection-1");
    }

    @Test
    void ignoresPauseWhenTheReplayJobHasAlreadyStopped() {
        MboReplayStreamService replay = mock(MboReplayStreamService.class);
        ReplayWebSocketHandler handler = new ReplayWebSocketHandler(JsonMapper.builder().build(), replay);
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("connection-1");

        assertThatCode(() -> handler.handleTextMessage(
            session, new TextMessage("{\"type\":\"replay_pause\",\"payload\":{}}")
        )).doesNotThrowAnyException();

        verify(replay).pause("connection-1");
    }

    @Test
    void forwardsTheCursorWhenAClientContinuesAReplayChunk() {
        MboReplayStreamService replay = mock(MboReplayStreamService.class);
        ReplayWebSocketHandler handler = new ReplayWebSocketHandler(JsonMapper.builder().build(), replay);
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("connection-1");
        when(replay.continueReplay("connection-1", new ReplayCursor("sha", "12", "1700000000000000000")))
            .thenReturn(true);

        assertThatCode(() -> handler.handleTextMessage(
            session,
            new TextMessage("{\"type\":\"replay_continue\",\"payload\":{\"cursor\":{"
                + "\"fileSha256\":\"sha\",\"sourceOrdinal\":\"12\","
                + "\"lastEventNs\":\"1700000000000000000\"}}}")
        )).doesNotThrowAnyException();

        verify(replay).continueReplay(
            "connection-1", new ReplayCursor("sha", "12", "1700000000000000000")
        );
    }
}
