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
                recs.setSuggestions(buildSmartSuggestions(recs.getRecommendations(), s.request(), s.suggestions()));

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

                List<Map<String, String>> messages = List.of(
                    Map.of("role", "system", "content", """
                        You are a pragmatic, data-driven media planning strategist with global expertise
                        across all markets. Your goal is to synthesize raw media placements
                        into a cohesive, actionable marketing plan — not a generic one.

                        Rules:
                        - Allocate budget_share_pct so the sum across all placements equals 100.
                        - Prioritise high-traffic outlets with strong audience alignment.
                        - Suggest the format that best serves the campaign objective for each outlet.
                        - The `notes` field must contain ONE concrete, non-obvious strategic tip
                          specific to this market and audience (e.g. day-of-week timing, seasonal
                          patterns, audience behaviour peculiarities for this country/sector).
                        - Return ONLY valid JSON, no markdown, no prose outside the JSON object.

                        Output schema (strict):
                        {
                          "objective": "<one sentence campaign goal>",
                          "placements": [
                            { "media_title": "<title>", "suggested_format": "<format>", "budget_share_pct": <integer 0-100> }
                          ],
                          "total_budget_note": "<e.g. $2,000 total — 3 placements as shown>",
                          "notes": "<one concrete, specific strategic tip>"
                        }
                        """),
                    Map.of("role", "user", "content",
                        "Create a media plan for the following campaign:\n\n" + context)
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
        cta.append(String.format("Found **%d** media placement%s.", total, total == 1 ? "" : "s"));
        cta.append(String.format(" %s — top: **%s** (%d%%)", quality, top.getTitle(), topScore));

        // Top outlet quick stats (traffic + cost) — full numbers, no truncation
        if (top.getSimilarwebVisits() != null && top.getSimilarwebVisits() > 0) {
            cta.append(String.format(" — %s monthly visits", formatVisits(top.getSimilarwebVisits())));
        }
        if (top.getCostUsd() != null) {
            cta.append(String.format(", $%.0f per placement", top.getCostUsd().doubleValue()));
        }
        cta.append(".");

        // Hint at what would sharpen results
        List<String> missing = new ArrayList<>();
        if (request.getBudgetUsd() == null) missing.add("budget");
        if (request.getFormatPreference() == null)
            missing.add("format (Article, Press Release, or Video)");
        if (!missing.isEmpty()) {
            cta.append(" For sharper results, share your ").append(String.join(" and ", missing)).append(".");
        }

        cta.append(" Say **\"create marketing plan\"** for a full channel strategy.");
        return cta.toString();
    }

    /**
     * Generates 3 suggestions based on the actual enriched results rather than
     * the router's pre-search guesses. Picks what would genuinely improve the results.
     */
    private List<String> buildSmartSuggestions(
            List<RecommendationResponseDTO.MediaItemDTO> recs,
            RecommendationRequestDTO request,
            List<String> routerFallback) {

        List<String> suggestions = new ArrayList<>();
        double budget = request.getBudgetUsd() != null ? request.getBudgetUsd() : Double.MAX_VALUE;

        if (!recs.isEmpty()) {
            // 1. Budget hint — if most results are over budget, suggest the min cost
            long overBudget = recs.stream()
                .filter(r -> r.getCostUsd() != null && r.getCostUsd().doubleValue() > budget)
                .count();
            if (overBudget > 0 && request.getBudgetUsd() != null) {
                recs.stream()
                    .filter(r -> r.getCostUsd() != null)
                    .mapToDouble(r -> r.getCostUsd().doubleValue())
                    .min()
                    .ifPresent(min -> {
                        int suggested = (int)(Math.ceil(min / 50) * 50);
                        if (suggested > budget)
                            suggestions.add("Set budget to $" + suggested + " to include more options");
                    });
            }

            // 2. Format diversity — show the most common format in results
            recs.stream()
                .map(RecommendationResponseDTO.MediaItemDTO::getSuggestedFormat)
                .filter(f -> f != null && !f.isBlank())
                .collect(java.util.stream.Collectors.groupingBy(f -> f, java.util.stream.Collectors.counting()))
                .entrySet().stream()
                .max(java.util.Map.Entry.comparingByValue())
                .map(java.util.Map.Entry::getKey)
                .ifPresent(topFormat -> {
                    if (request.getFormatPreference() == null)
                        suggestions.add("Filter by " + topFormat + " format only");
                });

            // 3. Show more if we got few results
            if (recs.size() < 5)
                suggestions.add("Increase to 15 results");
        }

        // 4. Always offer marketing plan
        if (!suggestions.contains("Create marketing plan"))
            suggestions.add("Create marketing plan");

        // 5. Fill remaining from router fallback
        for (String s : routerFallback) {
            if (suggestions.size() >= 3) break;
            if (!suggestions.contains(s)) suggestions.add(s);
        }

        return suggestions.subList(0, Math.min(3, suggestions.size()));
    }

    private static String formatVisits(long visits) {
        if (visits >= 1_000_000) return String.format("%.1fM", visits / 1_000_000.0);
        if (visits >= 1_000)     return String.format("%.0fK", visits / 1_000.0);
        return String.valueOf(visits);
    }

    private void saveRecommendationContext(String sessionId,
                                            RecommendationResponseDTO recs,
                                            RecommendationRequestDTO request) {
        try {
            String topTitles = recs.getRecommendations().stream().limit(3)
                .map(RecommendationResponseDTO.MediaItemDTO::getTitle)
                .collect(Collectors.joining(", "));

            // Structured param block — framed as authoritative active state,
            // not conversation history. Router reads this as ground truth for next query.
            StringBuilder context = new StringBuilder();
            context.append("categories=").append(request.getCategories()).append("\n");
            context.append("budget=").append(request.getBudgetUsd() != null
                ? "$" + String.format("%.0f", request.getBudgetUsd()) + " USD" : "not specified").append("\n");
            context.append("objective=").append(request.getCampaignObjective() != null
                ? request.getCampaignObjective() : "awareness").append("\n");
            context.append("format=").append(request.getFormatPreference() != null
                ? request.getFormatPreference() : "not specified").append("\n");
            if (request.getKeywords() != null && !request.getKeywords().isEmpty())
                context.append("keywords=").append(request.getKeywords()).append("\n");
            context.append("region=").append(request.getRegion() != null
                ? request.getRegion() : "not specified").append("\n");
            context.append("country=").append(request.getCountry() != null
                ? request.getCountry() : "not specified").append("\n");
            context.append("max_results=").append(request.getMaxResults()).append("\n");
            context.append("Last results shown: ").append(topTitles);

            // Also store full recs for marketing plan generation
            String fullContext = context.toString() + "\n\nFull placements:\n" +
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
