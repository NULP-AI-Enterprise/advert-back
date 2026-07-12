package com.advertising.service.debug;

import com.advertising.model.websocket.WebSocketMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Thin helper that builds DEBUG_EVENT WebSocket frames and pushes them to the
 * per-request debug sink without cluttering service code.
 */
@Slf4j
public final class DebugEvents {

    private DebugEvents() {}

    /** No-op sink used when debug is disabled or not wired. */
    public static final Consumer<WebSocketMessage> NOOP = msg -> {};

    /**
     * Emit a structured debug event.
     *
     * @param debug    consumer that routes events to the WebSocket sink (may be NOOP)
     * @param sessionId request session — attached to every frame
     * @param stage    top-level pipeline stage: "router" | "search" | "enrichment"
     * @param event    specific event name within the stage
     * @param label    human-readable one-liner shown in the debug panel header
     * @param data     arbitrary structured payload (Map, list, primitive — serialised to JSON)
     */
    public static void emit(Consumer<WebSocketMessage> debug, String sessionId,
                             String stage, String event, String label, Object data) {
        if (debug == null || debug == NOOP) return;
        try {
            debug.accept(WebSocketMessage.builder()
                .type(WebSocketMessage.MessageType.DEBUG_EVENT)
                .sessionId(sessionId)
                .payload(Map.of("stage", stage, "event", event, "label", label, "data", data))
                .build());
        } catch (Exception e) {
            log.debug("[Debug] failed to emit debug event: {}", e.getMessage());
        }
    }
}
