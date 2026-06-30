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
            Object cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                return objectMapper.convertValue(cached, new TypeReference<List<ChatMessage>>() {});
            }

            List<ChatMessage> messages = messageRepository
                .findLatestBySessionId(UUID.fromString(sessionId), limit);

            redisTemplate.opsForValue().set(cacheKey, messages, Duration.ofHours(ttlHours));
            return messages;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public void invalidateCache(String sessionId) {
        redisTemplate.delete(keyPrefix + sessionId);
    }
}
