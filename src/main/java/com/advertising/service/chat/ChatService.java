package com.advertising.service.chat;

import com.advertising.model.entity.ChatMessage;
import com.advertising.model.entity.ChatSession;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.repository.ChatMessageRepository;
import com.advertising.repository.ChatSessionRepository;
import com.advertising.service.enrichment.EnrichmentMechanismService;
import com.advertising.service.openai.AgenticLoopService;
import com.advertising.service.recommendation.RecEngineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.dto.RecommendationResponseDTO;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService {

    private final ChatSessionRepository sessionRepository;
    private final ChatMessageRepository messageRepository;
    private final AgenticLoopService agenticLoopService;
    private final RecEngineService recEngineService;
    private final EnrichmentMechanismService enrichmentMechanismService;
    private final ChatHistoryService chatHistoryService;

    public Flux<WebSocketMessage> processMessage(String sessionId, String userContent) {
        log.info("[Chat] processMessage session={} content={}", sessionId,
            userContent.substring(0, Math.min(80, userContent.length())));

        return Mono.fromRunnable(() -> doSave(sessionId, ChatMessage.MessageRole.user, userContent))
            .subscribeOn(Schedulers.boundedElastic())
            .then(Mono.fromRunnable(() -> chatHistoryService.invalidateCache(sessionId))
                .subscribeOn(Schedulers.boundedElastic()))
            .doOnSuccess(__ -> log.info("[Chat] calling AgenticLoop..."))
            .thenMany(agenticLoopService.decide(sessionId, userContent)
                .flatMapMany(decision -> handleDecision(sessionId, decision)))
            .doOnError(e -> log.error("[Chat] pipeline error: {}", e.getMessage(), e));
    }

    private Flux<WebSocketMessage> handleDecision(String sessionId, AgenticLoopService.AgentDecision decision) {
        log.info("[Chat] AgentDecision type={}", decision.getClass().getSimpleName());

        if (decision instanceof AgenticLoopService.AgentDecision.Clarify c) {
            log.info("[Chat] clarify → '{}'", c.question());
            return Mono.fromRunnable(() -> doSave(sessionId, ChatMessage.MessageRole.assistant, c.question()))
                .subscribeOn(Schedulers.boundedElastic())
                .thenReturn(WebSocketMessage.builder()
                    .type(WebSocketMessage.MessageType.CLARIFICATION_QUESTION)
                    .sessionId(sessionId)
                    .content(c.question())
                    .build())
                .flux();
        }

        AgenticLoopService.AgentDecision.Search s = (AgenticLoopService.AgentDecision.Search) decision;
        log.info("[Chat] search → categories={} objective={} audience={}",
            s.request().getCategories(),
            s.request().getCampaignObjective(),
            s.request().getTargetAudienceDescription());

        return recEngineService.findCandidates(s.request())
            .flatMap(candidates -> {
                log.info("[Chat] RecEngine returned {} raw candidates", candidates.size());
                return enrichmentMechanismService.enrich(candidates, s.request());
            })
            .flatMapMany(recs -> {
                log.info("[Chat] enriched recs: {}", recs.getRecommendations().size());

                String explanation = buildExplanation(recs.getRecommendations(), s.request());

                WebSocketMessage textMsg = WebSocketMessage.builder()
                    .type(WebSocketMessage.MessageType.ASSISTANT_MESSAGE)
                    .sessionId(sessionId)
                    .content(explanation)
                    .build();
                WebSocketMessage recsMsg = WebSocketMessage.builder()
                    .type(WebSocketMessage.MessageType.RECOMMENDATIONS_READY)
                    .sessionId(sessionId)
                    .payload(recs)
                    .build();
                return Flux.just(textMsg, recsMsg);
            });
    }

    private String buildExplanation(List<RecommendationResponseDTO.MediaItemDTO> recs,
                                     RecommendationRequestDTO request) {
        if (recs.isEmpty()) {
            return "На жаль, не вдалося знайти відповідні медіа-майданчики за вашим запитом. Спробуйте уточнити параметри кампанії.";
        }
        String topNames = recs.stream()
            .limit(3)
            .map(RecommendationResponseDTO.MediaItemDTO::getTitle)
            .collect(Collectors.joining(", "));
        int total = recs.size();

        // Use match_score from enrichment (V1) — take the best
        int bestScore = recs.stream()
            .mapToInt(item -> item.getMatchScore() != null ? item.getMatchScore() : 0)
            .max().orElse(0);
        String matchQuality = bestScore >= 75 ? "Висока релевантність" :
                              bestScore >= 50 ? "Хороша релевантність" : "Базовий підбір";

        String objective = request.getCampaignObjective() != null
            ? " для цілі «" + request.getCampaignObjective() + "»"
            : "";

        return String.format(
            "Знайдено %d медіа-майданчиків%s. %s. Найкращі варіанти: %s. " +
            "Кожен результат містить аналіз відповідності вашій кампанії.",
            total, objective, matchQuality, topNames
        );
    }

    private void doSave(String sessionId, ChatMessage.MessageRole role, String content) {
        ChatSession session = sessionRepository.findById(UUID.fromString(sessionId))
            .orElseThrow(() -> new IllegalArgumentException("Session not found: " + sessionId));
        messageRepository.save(ChatMessage.builder()
            .session(session).role(role).content(content).build());
    }
}
