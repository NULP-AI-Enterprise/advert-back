package com.advertising.service.chat;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.dto.RecommendationResponseDTO;
import com.advertising.model.entity.ChatMessage;
import com.advertising.model.entity.ChatSession;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.repository.ChatMessageRepository;
import com.advertising.repository.ChatSessionRepository;
import com.advertising.service.enrichment.EnrichmentMechanismService;
import com.advertising.service.openai.AgenticLoopService;
import com.advertising.service.openai.OpenAIService;
import com.advertising.service.recommendation.RecEngineService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private final OpenAIService openAIService;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String REC_CONTEXT_PREFIX = "rec:last:";
    private static final Duration REC_CONTEXT_TTL = Duration.ofHours(24);

    public Flux<WebSocketMessage> processMessage(String sessionId, String userContent, Object rawPayload) {
        log.info("[Chat] processMessage session={}", sessionId);

        // Extract device context from payload
        String deviceLocation = null;
        String deviceLanguage = null;
        try {
            if (rawPayload != null) {
                Map<String, Object> payload = objectMapper.convertValue(rawPayload, new TypeReference<>() {});
                Object dc = payload.get("deviceContext");
                if (dc instanceof Map<?, ?> dcMap) {
                    Object lat  = dcMap.get("lat");
                    Object lon  = dcMap.get("lon");
                    Object lang = dcMap.get("language");
                    if (lat != null && lon != null)
                        deviceLocation = lat + "," + lon;
                    if (lang != null)
                        deviceLanguage = lang.toString();
                }
            }
        } catch (Exception e) {
            log.debug("[Chat] could not extract device context: {}", e.getMessage());
        }

        final String finalDeviceLocation = deviceLocation;
        final String finalDeviceLanguage = deviceLanguage;

        // Load previous recommendation context from Redis
        String previousContext = loadPreviousContext(sessionId);

        return Mono.fromRunnable(() -> doSave(sessionId, ChatMessage.MessageRole.user, userContent))
            .subscribeOn(Schedulers.boundedElastic())
            .then(Mono.fromRunnable(() -> chatHistoryService.invalidateCache(sessionId))
                .subscribeOn(Schedulers.boundedElastic()))
            .thenMany(
                agenticLoopService.decide(sessionId, userContent, finalDeviceLocation, finalDeviceLanguage, previousContext)
                    .flatMapMany(decision -> handleDecision(sessionId, decision))
            )
            .doOnError(e -> log.error("[Chat] pipeline error: {}", e.getMessage(), e));
    }

    // Backward-compatible overload
    public Flux<WebSocketMessage> processMessage(String sessionId, String userContent) {
        return processMessage(sessionId, userContent, null);
    }

    private Flux<WebSocketMessage> handleDecision(String sessionId, AgenticLoopService.AgentDecision decision) {
        log.info("[Chat] decision={}", decision.getClass().getSimpleName());

        if (decision instanceof AgenticLoopService.AgentDecision.Clarify c) {
            return Mono.fromRunnable(() -> doSave(sessionId, ChatMessage.MessageRole.assistant, c.question()))
                .subscribeOn(Schedulers.boundedElastic())
                .thenReturn(WebSocketMessage.builder()
                    .type(WebSocketMessage.MessageType.CLARIFICATION_QUESTION)
                    .sessionId(sessionId)
                    .content(c.question())
                    .build())
                .flux();
        }

        if (decision instanceof AgenticLoopService.AgentDecision.Plan p) {
            return generateMarketingPlan(p.sessionId());
        }

        AgenticLoopService.AgentDecision.Search s = (AgenticLoopService.AgentDecision.Search) decision;
        log.info("[Chat] search categories={} region={}", s.request().getCategories(), s.request().getRegion());

        return recEngineService.findCandidates(s.request())
            .flatMap(candidates -> enrichmentMechanismService.enrich(candidates, s.request()))
            .flatMapMany(recs -> {
                // Save context to Redis for next query and marketing plan
                saveRecommendationContext(sessionId, recs, s.request());

                String ctaMessage = buildCtaMessage(recs.getRecommendations(), s.request());
                recs.setCtaMessage(ctaMessage);
                recs.setSuggestions(s.suggestions());

                // Add relaxation note if geo was broadened
                if (s.request().getRelaxationNote() != null) {
                    recs.setReasoning(s.request().getRelaxationNote());
                }

                WebSocketMessage recsMsg = WebSocketMessage.builder()
                    .type(WebSocketMessage.MessageType.RECOMMENDATIONS_READY)
                    .sessionId(sessionId)
                    .content(ctaMessage)
                    .payload(recs)
                    .build();
                return Flux.just(recsMsg);
            });
    }

    public Flux<WebSocketMessage> generateMarketingPlan(String sessionId) {
        return Mono.fromCallable(() -> loadPreviousContext(sessionId))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(context -> {
                if (context == null || context.isBlank()) {
                    return Mono.just(WebSocketMessage.builder()
                        .type(WebSocketMessage.MessageType.ASSISTANT_MESSAGE)
                        .sessionId(sessionId)
                        .content("No recent recommendations found. Please describe your campaign first to get media placements, then I can create a marketing plan.")
                        .build());
                }

                String prompt = """
                    Based on the following campaign context and media placements, create a lightweight media plan.
                    Return ONLY valid JSON:
                    {
                      "objective": "<one sentence campaign goal>",
                      "placements": [
                        { "media_title": "<title>", "suggested_format": "<format>", "budget_share_pct": <0-100> }
                      ],
                      "total_budget_note": "<e.g. $2,000 total — allocate as shown>",
                      "notes": "<one sentence strategic tip>"
                    }

                    Campaign context:
                    """ + context;

                List<Map<String, String>> messages = List.of(
                    Map.of("role", "system", "content",
                        "You are a media planning strategist. Return only valid JSON, no markdown."),
                    Map.of("role", "user", "content", prompt)
                );

                return openAIService.chatCompletionJson(messages)
                    .map(planJson -> WebSocketMessage.builder()
                        .type(WebSocketMessage.MessageType.MARKETING_PLAN_READY)
                        .sessionId(sessionId)
                        .payload(planJson)
                        .build())
                    .onErrorReturn(WebSocketMessage.builder()
                        .type(WebSocketMessage.MessageType.ASSISTANT_MESSAGE)
                        .sessionId(sessionId)
                        .content("Could not generate the marketing plan. Please try again.")
                        .build());
            })
            .flux();
    }

    private String buildCtaMessage(List<RecommendationResponseDTO.MediaItemDTO> recs,
                                    RecommendationRequestDTO request) {
        if (recs.isEmpty()) {
            return "No media placements found for your campaign. Try broadening your criteria — different category, wider region, or more general audience description.";
        }

        int total = recs.size();
        RecommendationResponseDTO.MediaItemDTO top = recs.get(0);
        int topScore = top.getMatchScore() != null ? top.getMatchScore() : 0;
        String quality = topScore >= 80 ? "Strong match" : topScore >= 60 ? "Good match" : "Partial match";

        StringBuilder cta = new StringBuilder();
        cta.append(String.format("Found %d media placement%s. %s — top: **%s** (%d%%)",
            total, total == 1 ? "" : "s", quality, top.getTitle(), topScore));

        // Add top match reason snippet if available
        if (top.getMatchReason() != null && !top.getMatchReason().isBlank()) {
            String snippet = top.getMatchReason().length() > 90
                ? top.getMatchReason().substring(0, 87) + "…"
                : top.getMatchReason();
            cta.append(". ").append(snippet);
        }

        // What would improve results
        List<String> missing = new ArrayList<>();
        if (request.getBudgetUsd() == null) missing.add("budget");
        if (request.getRegion() == null || request.getRegion().isBlank()) missing.add("target region");
        if (!missing.isEmpty()) {
            cta.append(" For sharper results, tell me your ").append(String.join(" or ", missing)).append(".");
        }

        cta.append(" I can also create a **Marketing Plan** — just say \"create marketing plan\".");
        return cta.toString();
    }

    private void saveRecommendationContext(String sessionId,
                                            RecommendationResponseDTO recs,
                                            RecommendationRequestDTO request) {
        try {
            String topTitles = recs.getRecommendations().stream().limit(3)
                .map(RecommendationResponseDTO.MediaItemDTO::getTitle)
                .collect(Collectors.joining(", "));

            String context = String.format(
                "Filters: categories=%s, region=%s, budget=$%.0f, objective=%s\nTop results shown: %s",
                request.getCategories(),
                request.getRegion() != null ? request.getRegion() : "not specified",
                request.getBudgetUsd() != null ? request.getBudgetUsd() : 0,
                request.getCampaignObjective() != null ? request.getCampaignObjective() : "not specified",
                topTitles
            );

            // Also store full recs for marketing plan generation
            String fullContext = context + "\n\nFull placements:\n" +
                recs.getRecommendations().stream()
                    .map(r -> String.format("- %s (%s, score=%d, format=%s, cost=$%s%s)",
                        r.getTitle(), r.getCategory(),
                        r.getMatchScore() != null ? r.getMatchScore() : 0,
                        r.getSuggestedFormat() != null ? r.getSuggestedFormat() : "TBD",
                        r.getCostUsd() != null ? r.getCostUsd() : "N/A",
                        r.getBudgetFit() != null ? ", " + r.getBudgetFit() : ""))
                    .collect(Collectors.joining("\n"));
            if (recs.getReasoning() != null && !recs.getReasoning().isBlank()) {
                fullContext += "\n\nStrategy reasoning: " + recs.getReasoning();
            }

            redisTemplate.opsForValue().set(REC_CONTEXT_PREFIX + sessionId, fullContext, REC_CONTEXT_TTL);
        } catch (Exception e) {
            log.warn("[Chat] failed to save rec context to Redis: {}", e.getMessage());
        }
    }

    private String loadPreviousContext(String sessionId) {
        try {
            Object cached = redisTemplate.opsForValue().get(REC_CONTEXT_PREFIX + sessionId);
            if (cached instanceof String s) return s;
            if (cached != null) return cached.toString();
        } catch (Exception e) {
            log.debug("[Chat] no previous context for session {}", sessionId);
        }
        return null;
    }

    private void doSave(String sessionId, ChatMessage.MessageRole role, String content) {
        ChatSession session = sessionRepository.findById(UUID.fromString(sessionId))
            .orElseThrow(() -> new IllegalArgumentException("Session not found: " + sessionId));
        messageRepository.save(ChatMessage.builder()
            .session(session).role(role).content(content).build());
    }
}
