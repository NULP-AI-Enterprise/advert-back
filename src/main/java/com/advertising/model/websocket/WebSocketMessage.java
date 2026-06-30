package com.advertising.model.websocket;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.OffsetDateTime;

/**
 * Unified envelope for all WebSocket frames (both directions).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WebSocketMessage {

    private MessageType type;
    private String sessionId;
    private String content;
    private Object payload;
    private String error;

    @Builder.Default
    private OffsetDateTime timestamp = OffsetDateTime.now();

    public enum MessageType {
        // Client → Server
        CHAT_MESSAGE,
        REQUEST_RECOMMENDATIONS,

        // Server → Client
        ASSISTANT_MESSAGE,
        ASSISTANT_STREAM_CHUNK,
        ASSISTANT_STREAM_END,
        RECOMMENDATIONS_READY,
        CLARIFICATION_QUESTION,
        ERROR,
        PING,
        PONG
    }
}
