package com.advertising.controller;

import com.advertising.model.entity.ChatSession;
import com.advertising.repository.ChatSessionRepository;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/chat")
@RequiredArgsConstructor
public class ChatController {

    private final ChatSessionRepository sessionRepository;

    /**
     * Creates a new chat session and returns its ID.
     * The frontend stores this and passes it in every WebSocket message.
     */
    @PostMapping("/sessions")
    public ResponseEntity<Map<String, String>> createSession(
            @RequestParam @NotBlank String userId) {

        ChatSession session = sessionRepository.save(
            ChatSession.builder()
                .userId(userId)
                .context(new HashMap<>())
                .build()
        );

        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body(Map.of("sessionId", session.getId().toString()));
    }

    @GetMapping("/sessions/{sessionId}")
    public ResponseEntity<ChatSession> getSession(@PathVariable UUID sessionId) {
        return sessionRepository.findByIdWithMessages(sessionId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/sessions/{sessionId}")
    public ResponseEntity<Void> deleteSession(@PathVariable UUID sessionId) {
        sessionRepository.deleteById(sessionId);
        return ResponseEntity.noContent().build();
    }
}
