package com.advertising.service.chat;

import com.advertising.model.entity.ChatMessage;
import com.advertising.repository.ChatMessageRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Reads chat history — Redis cache first, Postgres fallback.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChatHistoryService {

    private final ChatMessageRepository messageRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.redis.chat-history-key-prefix}")
    private String keyPrefix;

    @Value("${app.redis.session-ttl-hours}")
    private long ttlHours;

    public Mono<List<ChatMessage>> getRecentHistory(String sessionId, int limit) {
        String cacheKey = keyPrefix + sessionId;

        return Mono.fromCallable(() -> {
            log.debug("[ChatHistory] getRecentHistory session={} limit={} key={}", sessionId, limit, cacheKey);
            Object cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                List<ChatMessage> messages = objectMapper.convertValue(cached, new TypeReference<List<ChatMessage>>() {});
                log.info("[ChatHistory] cache HIT session={} messages={}", sessionId, messages.size());
                return messages;
            }

            log.info("[ChatHistory] cache MISS session={} — querying DB limit={}", sessionId, limit);
            List<ChatMessage> messages = messageRepository
                .findLatestBySessionId(UUID.fromString(sessionId), limit);
            log.info("[ChatHistory] DB returned {} messages for session={}", messages.size(), sessionId);
            if (!messages.isEmpty()) {
                log.debug("[ChatHistory] messages preview: {}",
                    messages.stream().limit(3)
                        .map(m -> "[" + m.getRole() + "] "
                            + (m.getContent().length() > 60
                                ? m.getContent().substring(0, 60) + "…"
                                : m.getContent()))
                        .toList());
            }
            redisTemplate.opsForValue().set(cacheKey, messages, Duration.ofHours(ttlHours));
            log.debug("[ChatHistory] cached {} messages session={} TTL={}h", messages.size(), sessionId, ttlHours);
            return messages;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public void invalidateCache(String sessionId) {
        log.debug("[ChatHistory] invalidating cache session={}", sessionId);
        redisTemplate.delete(keyPrefix + sessionId);
    }
}
