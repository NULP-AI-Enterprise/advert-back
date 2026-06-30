package com.advertising.service.auth;

import com.advertising.model.entity.MagicLink;
import com.advertising.model.entity.User;
import com.advertising.repository.MagicLinkRepository;
import com.advertising.repository.UserRepository;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final MagicLinkRepository magicLinkRepository;
    private final JavaMailSender mailSender;

    @Value("${app.jwt.secret}")
    private String jwtSecret;

    @Value("${app.frontend-url}")
    private String frontendUrl;

    @Value("${app.base-url:http://localhost:8080}")
    private String baseUrl;

    @Value("${spring.mail.username:noreply@localhost}")
    private String mailFrom;

    /**
     * Find-or-create user, generate magic link token, send email.
     */
    public Mono<Void> sendMagicLink(String email) {
        return Mono.fromRunnable(() -> {
            User user = userRepository.findByEmail(email)
                    .orElseGet(() -> userRepository.save(
                            User.builder().email(email).build()
                    ));

            String rawToken = UUID.randomUUID().toString().replace("-", "")
                    + UUID.randomUUID().toString().replace("-", "");

            MagicLink link = MagicLink.builder()
                    .user(user)
                    .token(rawToken)
                    .used(false)
                    .expiresAt(OffsetDateTime.now().plusMinutes(15))
                    .build();
            magicLinkRepository.save(link);

            String verifyUrl = baseUrl + "/api/auth/verify?token=" + rawToken;

            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(mailFrom);
            message.setTo(email);
            message.setSubject("Your sign-in link");
            message.setText("Click this link to sign in (valid 15 minutes):\n\n" + verifyUrl
                    + "\n\nIf you did not request this, please ignore this email.");
            mailSender.send(message);

            log.info("Magic link sent to {}", email);
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /**
     * Validate token, mark used, return signed JWT.
     */
    public String verifyToken(String token) {
        MagicLink link = magicLinkRepository.findByToken(token)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.UNAUTHORIZED, "Invalid or expired token"));

        if (link.isUsed()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Token already used");
        }
        if (link.getExpiresAt().isBefore(OffsetDateTime.now())) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Token has expired");
        }

        link.setUsed(true);
        magicLinkRepository.save(link);

        User user = link.getUser();
        user.setLastLoginAt(OffsetDateTime.now());
        userRepository.save(user);

        return buildJwt(user);
    }

    /**
     * Parse a JWT and return the user (for /auth/me or filter use).
     */
    public User getUserFromJwt(String jwt) {
        var claims = Jwts.parser()
                .verifyWith(Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8)))
                .build()
                .parseSignedClaims(jwt)
                .getPayload();

        UUID userId = UUID.fromString(claims.getSubject());
        return userRepository.findById(userId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "User not found"));
    }

    private String buildJwt(User user) {
        return Jwts.builder()
                .subject(user.getId().toString())
                .claim("email", user.getEmail())
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 7L * 24 * 3600 * 1000))
                .signWith(Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8)))
                .compact();
    }
}
