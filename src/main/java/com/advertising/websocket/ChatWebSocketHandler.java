package com.advertising.websocket;

import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.service.chat.ChatService;
import com.advertising.service.geo.IpGeoService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private final ChatService chatService;
    private final IpGeoService ipGeoService;
    private final ObjectMapper objectMapper;

    // wsSessionId → raw WebSocketSession (for bookkeeping)
    private final ConcurrentHashMap<String, WebSocketSession> activeSessions = new ConcurrentHashMap<>();
    // wsSessionId → thread-safe decorator, created once per connection
    private final ConcurrentHashMap<String, WebSocketSession> safeSessions = new ConcurrentHashMap<>();
    // wsSessionId → IP-detected country (populated asynchronously after connection)
    private final ConcurrentHashMap<String, String> sessionCountry = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession wsSession) {
        String id = wsSession.getId();
        activeSessions.put(id, wsSession);

        // One thread-safe decorator per connection — shared across all messages on this connection
        safeSessions.put(id, new ConcurrentWebSocketSessionDecorator(wsSession, 10_000, 512 * 1024));

        // Resolve IP → country in the background; does not block the connection handshake
        String ip = IpGeoService.extractIp(wsSession.getHandshakeHeaders(), wsSession.getRemoteAddress());
        if (ip != null) {
            ipGeoService.getCountry(ip)
                .subscribe(
                    country -> {
                        sessionCountry.put(id, country);
                        log.info("[WS] GeoIP: {} → {}", ip, country);
                    },
                    err -> log.debug("[WS] GeoIP error for {}: {}", ip, err.getMessage())
                );
        }

        log.info("WebSocket connection established: {}", id);
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
            inbound.getContent() != null
                ? inbound.getContent().substring(0, Math.min(80, inbound.getContent().length()))
                : null);

        switch (inbound.getType()) {
            case PING -> send(wsSession, WebSocketMessage.builder()
                .type(WebSocketMessage.MessageType.PONG).build());

            case CHAT_MESSAGE -> {
                log.info("[WS] routing CHAT_MESSAGE to ChatService, session={}", inbound.getSessionId());
                WebSocketSession safe = safeSessions.getOrDefault(wsSession.getId(), wsSession);
                String ipCountry = sessionCountry.get(wsSession.getId());
                chatService
                    .processMessage(inbound.getSessionId(), inbound.getContent(), inbound.getPayload(), ipCountry)
                    .subscribe(
                        msg -> { log.info("[WS] sending response type={}", msg.getType()); send(safe, msg); },
                        err -> { log.error("[WS] ChatService error: {}", err.getMessage(), err); sendError(safe, err.getMessage()); }
                    );
            }

            default -> sendError(wsSession, "Unknown message type: " + inbound.getType());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession wsSession, CloseStatus status) {
        String id = wsSession.getId();
        activeSessions.remove(id);
        safeSessions.remove(id);
        sessionCountry.remove(id);
        log.info("WebSocket closed: {} — {}", id, status);
    }

    @Override
    public void handleTransportError(WebSocketSession wsSession, Throwable exception) {
        log.error("WebSocket transport error for session {}", wsSession.getId(), exception);
        String id = wsSession.getId();
        activeSessions.remove(id);
        safeSessions.remove(id);
        sessionCountry.remove(id);
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
