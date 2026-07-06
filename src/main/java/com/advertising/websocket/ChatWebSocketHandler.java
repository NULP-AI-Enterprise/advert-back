package com.advertising.websocket;

import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.service.chat.ChatService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private final ChatService chatService;
    private final ObjectMapper objectMapper;

    // sessionId → WebSocketSession for sending back responses
    private final ConcurrentHashMap<String, WebSocketSession> activeSessions = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession wsSession) {
        activeSessions.put(wsSession.getId(), wsSession);
        log.info("WebSocket connection established: {}", wsSession.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession wsSession, TextMessage textMessage) throws Exception {
        log.info("[WS] raw frame from {}: {}", wsSession.getId(), textMessage.getPayload());

        WebSocketMessage inbound;
        try {
            inbound = objectMapper.readValue(textMessage.getPayload(), WebSocketMessage.class);
        } catch (Exception e) {
            log.error("[WS] parse error: {}", e.getMessage());
            sendError(wsSession, "Invalid message format");
            return;
        }

        log.info("[WS] type={} sessionId={} content={}",
            inbound.getType(), inbound.getSessionId(),
            inbound.getContent() != null ? inbound.getContent().substring(0, Math.min(80, inbound.getContent().length())) : null);

        switch (inbound.getType()) {
            case PING -> send(wsSession, WebSocketMessage.builder()
                .type(WebSocketMessage.MessageType.PONG).build());

            case CHAT_MESSAGE -> {
                log.info("[WS] routing CHAT_MESSAGE to ChatService, session={}", inbound.getSessionId());
                chatService
                    .processMessage(inbound.getSessionId(), inbound.getContent(), inbound.getPayload())
                    .subscribe(
                        msg -> { log.info("[WS] sending response type={}", msg.getType()); send(wsSession, msg); },
                        err -> { log.error("[WS] ChatService error: {}", err.getMessage(), err); sendError(wsSession, err.getMessage()); }
                    );
            }

            default -> sendError(wsSession, "Unknown message type: " + inbound.getType());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession wsSession, CloseStatus status) {
        activeSessions.remove(wsSession.getId());
        log.info("WebSocket closed: {} — {}", wsSession.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession wsSession, Throwable exception) {
        log.error("WebSocket transport error for session {}", wsSession.getId(), exception);
        activeSessions.remove(wsSession.getId());
    }

    private void send(WebSocketSession wsSession, WebSocketMessage message) {
        try {
            if (wsSession.isOpen()) {
                wsSession.sendMessage(new TextMessage(objectMapper.writeValueAsString(message)));
            }
        } catch (IOException e) {
            log.error("Failed to send WebSocket message to {}", wsSession.getId(), e);
        }
    }

    private void sendError(WebSocketSession wsSession, String errorMessage) {
        send(wsSession, WebSocketMessage.builder()
            .type(WebSocketMessage.MessageType.ERROR)
            .error(errorMessage)
            .build());
    }
}
