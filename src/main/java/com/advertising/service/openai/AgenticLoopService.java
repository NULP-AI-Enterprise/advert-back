package com.advertising.service.openai;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.ChatMessage;
import com.advertising.service.chat.ChatHistoryService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AgenticLoopService {

    private final OpenAIService openAIService;
    private final ChatHistoryService chatHistoryService;

    @Value("${app.recommendation.max-results}")
    private int defaultMaxResults;

    @Value("${app.recommendation.context-window:20}")
    private int contextWindow;

    private static final String ROUTER_SYSTEM_PROMPT = """
        You are an AI assistant helping users find the best media placements for their advertising campaigns.

        Before deciding, briefly reason through what you know and what's missing:
        1. What product/service is being advertised?
        2. Who is the target audience (age, interests, demographics)?
        3. What is the campaign objective (awareness/leads/conversions/engagement)?
        4. Is a region or location specified? If not, is device location available?

        Then choose one of three actions:

        ACTION "clarify" — ask ONE specific question to fill the most critical missing information.
        ACTION "search"  — you have enough context; return structured SQL search parameters.
        ACTION "plan"    — user explicitly asked to create a marketing plan for already-found placements.

        You have enough context to search when you know:
        - The product/service being advertised
        - The target audience (demographics, interests)
        - The campaign objective

        For "search", also generate 3 short follow-up suggestions the user might want to say next.

        Respond ONLY in valid JSON with this exact schema:
        {
          "reasoning": "<2-3 sentences: what you know, what action you chose and why>",
          "action": "clarify" | "search" | "plan",
          "question": "<string, only when action=clarify>",
          "search_params": {
            "categories": ["<string>"],
            "target_audience_description": "<string>",
            "age_range": { "min": 0, "max": 0 },
            "budget_usd": 0,
            "campaign_objective": "awareness|leads|conversions|engagement",
            "region": "<string>",
            "max_results": 10
          },
          "suggestions": ["<short follow-up prompt>", "<short follow-up prompt>", "<short follow-up prompt>"]
        }
        """;

    public Mono<AgentDecision> decide(String sessionId, String userMessage,
                                      String deviceLocation, String deviceLanguage,
                                      String previousContext) {
        log.info("[Agentic] decide() session={}", sessionId);
        return chatHistoryService.getRecentHistory(sessionId, contextWindow)
            .flatMap(history -> {
                log.info("[Agentic] history={} msgs, calling OpenAI", history.size());
                List<Map<String, String>> messages = buildMessageList(
                    history, userMessage, deviceLocation, deviceLanguage, previousContext);
                return openAIService.chatCompletionJson(messages);
            })
            .map(json -> {
                String reasoning = json.path("reasoning").asText("");
                if (!reasoning.isBlank()) log.info("[Agentic] reasoning: {}", reasoning);
                return parseDecision(json, sessionId);
            })
            .doOnError(e -> log.error("[Agentic] OpenAI call failed: {}", e.getMessage(), e));
    }

    // Backward-compatible overload
    public Mono<AgentDecision> decide(String sessionId, String userMessage) {
        return decide(sessionId, userMessage, null, null, null);
    }

    private List<Map<String, String>> buildMessageList(
            List<ChatMessage> history, String newUserMessage,
            String deviceLocation, String deviceLanguage, String previousContext) {

        StringBuilder systemContent = new StringBuilder(ROUTER_SYSTEM_PROMPT);

        if ((deviceLocation != null && !deviceLocation.isBlank())
                || (deviceLanguage != null && !deviceLanguage.isBlank())) {
            systemContent.append("\nDevice context (use as fallback if user hasn't specified):");
            if (deviceLocation != null && !deviceLocation.isBlank())
                systemContent.append("\n- Location: ").append(deviceLocation);
            if (deviceLanguage != null && !deviceLanguage.isBlank())
                systemContent.append("\n- Language: ").append(deviceLanguage);
        }

        if (previousContext != null && !previousContext.isBlank()) {
            systemContent.append("\n\nPrevious search context (use to avoid repeating results):\n")
                         .append(previousContext);
        }

        List<Map<String, String>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", systemContent.toString()));
        history.forEach(msg -> messages.add(
            Map.of("role", msg.getRole().name().toLowerCase(), "content", msg.getContent())
        ));
        messages.add(Map.of("role", "user", "content", newUserMessage));
        return messages;
    }

    private AgentDecision parseDecision(JsonNode json, String sessionId) {
        String action = json.path("action").asText("clarify");

        List<String> suggestions = new ArrayList<>();
        json.path("suggestions").forEach(n -> suggestions.add(n.asText()));

        if ("plan".equals(action)) {
            return AgentDecision.plan(sessionId);
        }

        if ("search".equals(action)) {
            JsonNode params = json.path("search_params");

            RecommendationRequestDTO.RecommendationRequestDTOBuilder builder =
                RecommendationRequestDTO.builder().sessionId(sessionId);

            if (params.has("categories")) {
                List<String> cats = new ArrayList<>();
                params.path("categories").forEach(n -> cats.add(n.asText()));
                builder.categories(cats);
            }

            String audienceDesc = params.path("target_audience_description").asText(null);
            if (audienceDesc != null && !audienceDesc.isBlank())
                builder.targetAudienceDescription(audienceDesc);

            JsonNode ageNode = params.path("age_range");
            if (!ageNode.isMissingNode() && !ageNode.isNull()) {
                builder.ageRange(RecommendationRequestDTO.AgeRange.builder()
                    .min(ageNode.path("min").asInt(0))
                    .max(ageNode.path("max").asInt(0))
                    .build());
            }

            if (params.has("budget_usd") && !params.path("budget_usd").isNull())
                builder.budgetUsd(params.path("budget_usd").asDouble());

            String objective = params.path("campaign_objective").asText(null);
            if (objective != null && !objective.isBlank()) builder.campaignObjective(objective);

            String region = params.path("region").asText(null);
            if (region != null && !region.isBlank()) builder.region(region);

            int maxResults = params.path("max_results").asInt(defaultMaxResults);
            builder.maxResults(maxResults > 0 ? maxResults : defaultMaxResults);

            return AgentDecision.search(builder.build(), suggestions);
        }

        return AgentDecision.clarify(json.path("question").asText(
            "Could you tell me more about your campaign goals?"
        ));
    }

    public sealed interface AgentDecision permits AgentDecision.Clarify, AgentDecision.Search, AgentDecision.Plan {

        record Clarify(String question) implements AgentDecision {}
        record Search(RecommendationRequestDTO request, List<String> suggestions) implements AgentDecision {}
        record Plan(String sessionId) implements AgentDecision {}

        static AgentDecision clarify(String question) { return new Clarify(question); }
        static AgentDecision search(RecommendationRequestDTO req, List<String> suggestions) {
            return new Search(req, suggestions);
        }
        static AgentDecision plan(String sessionId) { return new Plan(sessionId); }
    }
}
