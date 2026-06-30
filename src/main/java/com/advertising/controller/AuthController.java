package com.advertising.controller;

import com.advertising.model.entity.User;
import com.advertising.service.auth.AuthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @Value("${app.frontend-url}")
    private String frontendUrl;

    /**
     * POST /auth/magic-link
     * Body: { "email": "user@example.com" }
     * Sends a magic-link email and returns 200 OK.
     */
    @PostMapping("/magic-link")
    public Mono<ResponseEntity<Map<String, String>>> sendMagicLink(
            @RequestBody Map<String, String> body) {

        String email = body.get("email");
        if (email == null || email.isBlank()) {
            return Mono.just(ResponseEntity
                    .badRequest()
                    .body(Map.of("error", "email is required")));
        }

        return authService.sendMagicLink(email.trim().toLowerCase())
                .thenReturn(ResponseEntity.ok(Map.of("message", "Magic link sent to " + email)));
    }

    /**
     * GET /auth/verify?token=...
     * Validates the token and redirects to frontend with JWT in fragment.
     */
    @GetMapping("/verify")
    public ResponseEntity<Void> verifyToken(@RequestParam String token) {
        try {
            String jwt = authService.verifyToken(token);
            URI redirectUri = URI.create(frontendUrl + "/auth/callback#token=" + jwt);
            return ResponseEntity.status(HttpStatus.FOUND)
                    .header(HttpHeaders.LOCATION, redirectUri.toString())
                    .build();
        } catch (ResponseStatusException e) {
            URI errorUri = URI.create(frontendUrl + "/auth/error?reason=" + e.getReason());
            return ResponseEntity.status(HttpStatus.FOUND)
                    .header(HttpHeaders.LOCATION, errorUri.toString())
                    .build();
        }
    }

    /**
     * GET /auth/me
     * Returns the authenticated user's info. Requires Bearer JWT.
     */
    @GetMapping("/me")
    public ResponseEntity<Map<String, Object>> me(
            @RequestHeader(value = "Authorization", required = false) String authHeader) {

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Missing or invalid Authorization header");
        }

        String token = authHeader.substring(7);
        try {
            User user = authService.getUserFromJwt(token);
            return ResponseEntity.ok(Map.of(
                    "id", user.getId().toString(),
                    "email", user.getEmail(),
                    "displayName", user.getDisplayName() != null ? user.getDisplayName() : ""
            ));
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid token");
        }
    }
}
