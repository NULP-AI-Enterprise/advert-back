package com.advertising.controller;

import com.advertising.model.entity.ChatMessage;
import com.advertising.model.entity.ChatSession;
import com.advertising.model.entity.User;
import com.advertising.repository.ChatMessageRepository;
import com.advertising.repository.ChatSessionRepository;
import com.advertising.repository.UserRepository;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/chat")
@RequiredArgsConstructor
public class ChatController {

    private final ChatSessionRepository sessionRepository;
    private final ChatMessageRepository messageRepository;
    private final UserRepository userRepository;

    @Value("${app.jwt.secret}")
    private String jwtSecret;

    /**
     * Creates a new chat session.
     * If a valid JWT is provided, the session is linked to that user.
     */
    @PostMapping("/sessions")
    public ResponseEntity<Map<String, String>> createSession(
            @RequestParam @NotBlank String userId,
            @RequestHeader(value = "Authorization", required = false) String authHeader) {

        ChatSession.ChatSessionBuilder builder = ChatSession.builder()
                .userId(userId)
                .context(new HashMap<>());

        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            extractUserFromJwt(authHeader.substring(7))
                    .ifPresent(builder::user);
        }

        ChatSession session = sessionRepository.save(builder.build());

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(Map.of("sessionId", session.getId().toString()));
    }

    /**
     * GET /chat/sessions?userId={userId}
     * Lists sessions. If a valid JWT is present, returns sessions for the authenticated user.
     * Otherwise falls back to userId parameter.
     */
    @GetMapping("/sessions")
    public ResponseEntity<List<Map<String, Object>>> listSessions(
            @RequestParam(required = false) String userId,
            @RequestHeader(value = "Authorization", required = false) String authHeader) {

        List<ChatSession> sessions;

        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            Optional<User> userOpt = extractUserFromJwt(authHeader.substring(7));
            if (userOpt.isPresent()) {
                sessions = sessionRepository.findByUserOrderByUpdatedAtDesc(userOpt.get());
            } else {
                sessions = userId != null
                        ? sessionRepository.findByUserIdOrderByUpdatedAtDesc(userId)
                        : Collections.emptyList();
            }
        } else {
            sessions = userId != null
                    ? sessionRepository.findByUserIdOrderByUpdatedAtDesc(userId)
                    : Collections.emptyList();
        }

        List<Map<String, Object>> result = sessions.stream().map(s -> {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("id", s.getId().toString());
            map.put("title", s.getTitle());
            map.put("userId", s.getUserId());
            map.put("createdAt", s.getCreatedAt());
            map.put("updatedAt", s.getUpdatedAt());
            map.put("messageCount", s.getMessages().size());
            return map;
        }).toList();

        return ResponseEntity.ok(result);
    }

    /**
     * GET /chat/sessions/{sessionId}
     * Returns the full session with messages.
     */
    @GetMapping("/sessions/{sessionId}")
    public ResponseEntity<ChatSession> getSession(@PathVariable UUID sessionId) {
        return sessionRepository.findByIdWithMessages(sessionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /chat/sessions/{sessionId}/messages
     * Returns all messages for a session in chronological order.
     */
    @GetMapping("/sessions/{sessionId}/messages")
    public ResponseEntity<List<ChatMessage>> getMessages(@PathVariable UUID sessionId) {
        if (!sessionRepository.existsById(sessionId)) {
            return ResponseEntity.notFound().build();
        }
        List<ChatMessage> messages = messageRepository.findBySessionIdOrdered(sessionId);
        return ResponseEntity.ok(messages);
    }

    /**
     * DELETE /chat/sessions/{sessionId}
     * Deletes a session after deleting its messages (cascade).
     */
    @DeleteMapping("/sessions/{sessionId}")
    public ResponseEntity<Void> deleteSession(@PathVariable UUID sessionId) {
        if (!sessionRepository.existsById(sessionId)) {
            return ResponseEntity.notFound().build();
        }
        // Messages have CascadeType.ALL so deleteById handles it
        sessionRepository.deleteById(sessionId);
        return ResponseEntity.noContent().build();
    }

    /**
     * PATCH /chat/sessions/{sessionId}/title
     * Body: { "title": "New title" }
     */
    @PatchMapping("/sessions/{sessionId}/title")
    public ResponseEntity<Map<String, String>> updateTitle(
            @PathVariable UUID sessionId,
            @RequestBody Map<String, String> body) {

        String newTitle = body.get("title");
        if (newTitle == null || newTitle.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "title is required");
        }

        ChatSession session = sessionRepository.findById(sessionId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Session not found"));

        session.setTitle(newTitle.trim());
        sessionRepository.save(session);

        return ResponseEntity.ok(Map.of("title", session.getTitle()));
    }

    // --- helpers ---

    private Optional<User> extractUserFromJwt(String token) {
        try {
            Claims claims = Jwts.parser()
                    .verifyWith(Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8)))
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();
            UUID userId = UUID.fromString(claims.getSubject());
            return userRepository.findById(userId);
        } catch (JwtException | IllegalArgumentException e) {
            log.debug("Could not extract user from JWT: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
