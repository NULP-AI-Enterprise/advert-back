package com.advertising.controller;

import com.advertising.model.entity.ChatMessage;
import com.advertising.model.entity.ChatSession;
import com.advertising.repository.ChatMessageRepository;
import com.advertising.repository.ChatSessionRepository;
import com.advertising.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.OffsetDateTime;
import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ChatController.class)
@Import(TestSecurityConfig.class)
@ActiveProfiles("test")
@DisplayName("ChatController")
class ChatControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockBean ChatSessionRepository sessionRepository;
    @MockBean ChatMessageRepository messageRepository;
    @MockBean UserRepository        userRepository;

    private UUID sessionId;
    private ChatSession session;

    @BeforeEach
    void setUp() {
        sessionId = UUID.randomUUID();
        session = ChatSession.builder()
                .id(sessionId)
                .userId("user-123")
                .title("Test session")
                .messages(new ArrayList<>())
                .context(new HashMap<>())
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .build();
    }

    // ──────────────────────────────────────────────────────
    // POST /chat/sessions
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("POST /chat/sessions")
    class CreateSession {

        @Test
        @DisplayName("creates session and returns 201 with sessionId")
        void validUserId_returns201() throws Exception {
            when(sessionRepository.save(any(ChatSession.class))).thenReturn(session);

            mockMvc.perform(post("/chat/sessions")
                            .param("userId", "user-123"))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.sessionId", is(sessionId.toString())));

            verify(sessionRepository).save(any(ChatSession.class));
        }

        @Test
        @DisplayName("returns 400 when userId param is missing")
        void missingUserId_returns400() throws Exception {
            mockMvc.perform(post("/chat/sessions"))
                    .andExpect(status().isBadRequest());

            verifyNoInteractions(sessionRepository);
        }
    }

    // ──────────────────────────────────────────────────────
    // GET /chat/sessions
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("GET /chat/sessions")
    class ListSessions {

        @Test
        @DisplayName("returns session list for given userId")
        void validUserId_returnsList() throws Exception {
            when(sessionRepository.findByUserIdOrderByUpdatedAtDesc("user-123"))
                    .thenReturn(List.of(session));

            mockMvc.perform(get("/chat/sessions").param("userId", "user-123"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(1)))
                    .andExpect(jsonPath("$[0].id", is(sessionId.toString())))
                    .andExpect(jsonPath("$[0].title", is("Test session")));
        }

        @Test
        @DisplayName("returns empty list when no userId and no JWT")
        void noUserId_returnsEmptyList() throws Exception {
            mockMvc.perform(get("/chat/sessions"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(0)));
        }
    }

    // ──────────────────────────────────────────────────────
    // GET /chat/sessions/{sessionId}
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("GET /chat/sessions/{id}")
    class GetSession {

        @Test
        @DisplayName("returns 200 with full session for existing id")
        void existingId_returns200() throws Exception {
            when(sessionRepository.findByIdWithMessages(sessionId)).thenReturn(Optional.of(session));

            mockMvc.perform(get("/chat/sessions/{id}", sessionId))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.userId", is("user-123")));
        }

        @Test
        @DisplayName("returns 404 for unknown session id")
        void unknownId_returns404() throws Exception {
            UUID unknown = UUID.randomUUID();
            when(sessionRepository.findByIdWithMessages(unknown)).thenReturn(Optional.empty());

            mockMvc.perform(get("/chat/sessions/{id}", unknown))
                    .andExpect(status().isNotFound());
        }
    }

    // ──────────────────────────────────────────────────────
    // GET /chat/sessions/{sessionId}/messages
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("GET /chat/sessions/{id}/messages")
    class GetMessages {

        @Test
        @DisplayName("returns ordered message list for existing session")
        void existingSession_returnsMessages() throws Exception {
            ChatMessage msg = ChatMessage.builder()
                    .id(UUID.randomUUID())
                    .content("Hello")
                    .role(ChatMessage.MessageRole.user)
                    .session(session)
                    .createdAt(OffsetDateTime.now())
                    .build();

            when(sessionRepository.existsById(sessionId)).thenReturn(true);
            when(messageRepository.findBySessionIdOrdered(sessionId)).thenReturn(List.of(msg));

            mockMvc.perform(get("/chat/sessions/{id}/messages", sessionId))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(1)))
                    .andExpect(jsonPath("$[0].content", is("Hello")));
        }

        @Test
        @DisplayName("returns 404 when session does not exist")
        void unknownSession_returns404() throws Exception {
            UUID unknown = UUID.randomUUID();
            when(sessionRepository.existsById(unknown)).thenReturn(false);

            mockMvc.perform(get("/chat/sessions/{id}/messages", unknown))
                    .andExpect(status().isNotFound());
        }
    }

    // ──────────────────────────────────────────────────────
    // DELETE /chat/sessions/{sessionId}
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("DELETE /chat/sessions/{id}")
    class DeleteSession {

        @Test
        @DisplayName("deletes session and returns 204")
        void existingSession_returns204() throws Exception {
            when(sessionRepository.existsById(sessionId)).thenReturn(true);

            mockMvc.perform(delete("/chat/sessions/{id}", sessionId))
                    .andExpect(status().isNoContent());

            verify(sessionRepository).deleteById(sessionId);
        }

        @Test
        @DisplayName("returns 404 when session does not exist")
        void unknownSession_returns404() throws Exception {
            UUID unknown = UUID.randomUUID();
            when(sessionRepository.existsById(unknown)).thenReturn(false);

            mockMvc.perform(delete("/chat/sessions/{id}", unknown))
                    .andExpect(status().isNotFound());

            verify(sessionRepository, never()).deleteById(any());
        }
    }

    // ──────────────────────────────────────────────────────
    // PATCH /chat/sessions/{sessionId}/title
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("PATCH /chat/sessions/{id}/title")
    class UpdateTitle {

        @Test
        @DisplayName("updates title and returns 200 with new title")
        void validTitle_returns200() throws Exception {
            when(sessionRepository.findById(sessionId)).thenReturn(Optional.of(session));
            when(sessionRepository.save(any())).thenReturn(session);

            mockMvc.perform(patch("/chat/sessions/{id}/title", sessionId)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(Map.of("title", "My Campaign"))))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.title", is("My Campaign")));
        }

        @Test
        @DisplayName("returns 400 when title is blank")
        void blankTitle_returns400() throws Exception {
            mockMvc.perform(patch("/chat/sessions/{id}/title", sessionId)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(Map.of("title", "  "))))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("returns 400 when title field is missing")
        void missingTitle_returns400() throws Exception {
            mockMvc.perform(patch("/chat/sessions/{id}/title", sessionId)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("returns 404 when session does not exist")
        void unknownSession_returns404() throws Exception {
            UUID unknown = UUID.randomUUID();
            when(sessionRepository.findById(unknown)).thenReturn(Optional.empty());

            mockMvc.perform(patch("/chat/sessions/{id}/title", unknown)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(Map.of("title", "New"))))
                    .andExpect(status().isNotFound());
        }
    }
}
