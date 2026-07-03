package com.advertising.controller;

import com.advertising.model.entity.User;
import com.advertising.service.auth.AuthService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;

@WebMvcTest(AuthController.class)
@Import(TestSecurityConfig.class)
@ActiveProfiles("test")
@DisplayName("AuthController")
class AuthControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;
    @MockBean  AuthService authService;

    private static final String JWT_SECRET =
            "test-secret-key-must-be-at-least-32-chars-long!!";

    private String buildJwt(UUID userId, String email) {
        return Jwts.builder()
                .subject(userId.toString())
                .claim("email", email)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 86_400_000L))
                .signWith(Keys.hmacShaKeyFor(JWT_SECRET.getBytes(StandardCharsets.UTF_8)))
                .compact();
    }

    // ──────────────────────────────────────────────────────
    // POST /auth/magic-link
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("POST /auth/magic-link")
    class SendMagicLink {

        @BeforeEach
        void setUp() {
            when(authService.sendMagicLink(anyString())).thenReturn(Mono.empty());
        }

        @Test
        @DisplayName("returns 200 and message when email is valid")
        void validEmail_returns200() throws Exception {
            var mvcResult = mockMvc.perform(post("/auth/magic-link")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(Map.of("email", "user@example.com"))))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(mvcResult))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.message", containsString("user@example.com")));

            verify(authService).sendMagicLink("user@example.com");
        }

        @Test
        @DisplayName("normalises email to lowercase before sending")
        void uppercaseEmail_isSentLowercase() throws Exception {
            var mvcResult = mockMvc.perform(post("/auth/magic-link")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(Map.of("email", "USER@EXAMPLE.COM"))))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(mvcResult))
                    .andExpect(status().isOk());

            verify(authService).sendMagicLink("user@example.com");
        }

        @Test
        @DisplayName("returns 400 when email field is missing")
        void missingEmail_returns400() throws Exception {
            var mvcResult = mockMvc.perform(post("/auth/magic-link")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{}"))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(mvcResult))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error").exists());

            verifyNoInteractions(authService);
        }

        @Test
        @DisplayName("returns 400 when email field is blank")
        void blankEmail_returns400() throws Exception {
            var mvcResult = mockMvc.perform(post("/auth/magic-link")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(Map.of("email", "   "))))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(mvcResult))
                    .andExpect(status().isBadRequest());

            verifyNoInteractions(authService);
        }
    }

    // ──────────────────────────────────────────────────────
    // GET /auth/verify
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("GET /auth/verify")
    class VerifyToken {

        @Test
        @DisplayName("redirects to frontend callback with JWT on valid token")
        void validToken_redirectsWithJwt() throws Exception {
            String jwt = "signed.jwt.token";
            when(authService.verifyToken("goodtoken")).thenReturn(jwt);

            mockMvc.perform(get("/auth/verify").param("token", "goodtoken"))
                    .andExpect(status().isFound())
                    .andExpect(header().string("Location",
                            containsString("/auth/callback#token=" + jwt)));
        }

        @Test
        @DisplayName("redirects to frontend error page on invalid token")
        void invalidToken_redirectsToError() throws Exception {
            when(authService.verifyToken("badtoken"))
                    .thenThrow(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid or expired token"));

            mockMvc.perform(get("/auth/verify").param("token", "badtoken"))
                    .andExpect(status().isFound())
                    .andExpect(header().string("Location", containsString("/auth/error")));
        }

        @Test
        @DisplayName("returns 400 when token param is missing")
        void missingToken_returns400() throws Exception {
            mockMvc.perform(get("/auth/verify"))
                    .andExpect(status().isBadRequest());
        }
    }

    // ──────────────────────────────────────────────────────
    // GET /auth/me
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("GET /auth/me")
    class GetCurrentUser {

        private UUID userId;
        private String jwt;

        @BeforeEach
        void setUp() {
            userId = UUID.randomUUID();
            jwt = buildJwt(userId, "alice@example.com");

            User user = User.builder()
                    .id(userId)
                    .email("alice@example.com")
                    .displayName("Alice")
                    .build();
            when(authService.getUserFromJwt(jwt)).thenReturn(user);
        }

        @Test
        @DisplayName("returns user info for valid Bearer token")
        void validJwt_returnsUser() throws Exception {
            mockMvc.perform(get("/auth/me")
                            .header("Authorization", "Bearer " + jwt))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id", is(userId.toString())))
                    .andExpect(jsonPath("$.email", is("alice@example.com")))
                    .andExpect(jsonPath("$.displayName", is("Alice")));
        }

        @Test
        @DisplayName("returns 401 when Authorization header is absent")
        void noAuthHeader_returns401() throws Exception {
            mockMvc.perform(get("/auth/me"))
                    .andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("returns 401 when token scheme is not Bearer")
        void basicScheme_returns401() throws Exception {
            mockMvc.perform(get("/auth/me")
                            .header("Authorization", "Basic dXNlcjpwYXNz"))
                    .andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("returns 401 when JWT is invalid or expired")
        void invalidJwt_returns401() throws Exception {
            when(authService.getUserFromJwt("bad.token")).thenThrow(new RuntimeException("bad"));

            mockMvc.perform(get("/auth/me")
                            .header("Authorization", "Bearer bad.token"))
                    .andExpect(status().isUnauthorized());
        }

        @Test
        @DisplayName("returns empty string for displayName when it is null")
        void nullDisplayName_returnsEmptyString() throws Exception {
            User user = User.builder()
                    .id(userId)
                    .email("alice@example.com")
                    .displayName(null)
                    .build();
            when(authService.getUserFromJwt(jwt)).thenReturn(user);

            mockMvc.perform(get("/auth/me")
                            .header("Authorization", "Bearer " + jwt))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.displayName", is("")));
        }
    }
}
