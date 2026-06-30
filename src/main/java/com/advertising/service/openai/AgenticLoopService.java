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

/**
 * Orchestrates the Agentic Loop (V1):
 *  1. Builds conversation context from history
 *  2. Asks LLM to decide: ask clarifying question OR emit structured SQL search params
 *  3. Returns either a clarification question (String) or a structured RecommendationRequestDTO
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AgenticLoopService {

    private final OpenAIService openAIService;
    private final ChatHistoryService chatHistoryService;

    @Value("${app.recommendation.max-results}")
    private int maxResults;

    private static final String ROUTER_SYSTEM_PROMPT = """
        You are an AI assistant helping users find the best media placements for their advertising campaigns.

        Your job is to analyze the conversation history and decide one of two actions:

        ACTION A — "clarify": You need more information. Return a single clarifying question.
        ACTION B — "search": You have enough context. Return structured search parameters for SQL-based filtering.

        You have enough context to search when you know:
        - The product/service being advertised
        - The target audience (demographics, interests)
        - The campaign objective (awareness, leads, conversions, or engagement)
        - Approximate budget (optional but helpful)

        Respond ONLY in valid JSON with this exact schema:
        {
          "action": "clarify" | "search",
          "question": "string (only when action=clarify)",
          "search_params": {
            "categories": ["string"],
            "target_audience_description": "string",
            "age_range": { "min": number, "max": number },
            "budget_usd": number,
            "campaign_objective": "awareness|leads|conversions|engagement",
            "region": "string"
          }
        }
        """;

    /**
     * Core agentic decision: should we ask for more info or trigger the rec engine?
     *
     * @return Mono wrapping either a clarification question (String) or RecommendationRequestDTO
     */
    public Mono<AgentDecision> decide(String sessionId, String userMessage) {
        log.info("[Agentic] decide() session={}", sessionId);
        return chatHistoryService.getRecentHistory(sessionId, 10)
            .flatMap(history -> {
                log.info("[Agentic] history loaded: {} msgs, calling OpenAI...", history.size());
                List<Map<String, String>> messages = buildMessageList(history, userMessage);
                return openAIService.chatCompletionJson(messages);
            })
            .map(json -> {
                log.info("[Agentic] OpenAI response: {}", json);
                return parseDecision(json, sessionId);
            })
            .doOnError(e -> log.error("[Agentic] OpenAI call failed: {}", e.getMessage(), e));
    }

    private List<Map<String, String>> buildMessageList(
            List<ChatMessage> history, String newUserMessage) {

        List<Map<String, String>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", ROUTER_SYSTEM_PROMPT));

        history.forEach(msg -> messages.add(
            Map.of("role", msg.getRole().name(), "content", msg.getContent())
        ));

        messages.add(Map.of("role", "user", "content", newUserMessage));
        return messages;
    }

    private AgentDecision parseDecision(JsonNode json, String sessionId) {
        String action = json.path("action").asText("clarify");

        if ("search".equals(action)) {
            JsonNode params = json.path("search_params");

            RecommendationRequestDTO.RecommendationRequestDTOBuilder builder =
                RecommendationRequestDTO.builder()
                    .sessionId(sessionId)
                    .maxResults(maxResults);

            // categories
            if (params.has("categories")) {
                List<String> cats = new ArrayList<>();
                params.path("categories").forEach(n -> cats.add(n.asText()));
                builder.categories(cats);
            }

            // target audience description
            String audienceDesc = params.path("target_audience_description").asText(null);
            if (audienceDesc != null && !audienceDesc.isBlank()) {
                builder.targetAudienceDescription(audienceDesc);
            }

            // age range
            JsonNode ageRangeNode = params.path("age_range");
            if (!ageRangeNode.isMissingNode() && !ageRangeNode.isNull()) {
                RecommendationRequestDTO.AgeRange ageRange = RecommendationRequestDTO.AgeRange.builder()
                    .min(ageRangeNode.path("min").asInt(0))
                    .max(ageRangeNode.path("max").asInt(0))
                    .build();
                builder.ageRange(ageRange);
            }

            // budget
            if (params.has("budget_usd") && !params.path("budget_usd").isNull()) {
                builder.budgetUsd(params.path("budget_usd").asDouble());
            }

            // campaign objective
            String objective = params.path("campaign_objective").asText(null);
            if (objective != null && !objective.isBlank()) {
                builder.campaignObjective(objective);
            }

            // region
            String region = params.path("region").asText(null);
            if (region != null && !region.isBlank()) {
                builder.region(region);
            }

            return AgentDecision.search(builder.build());
        }

        return AgentDecision.clarify(json.path("question").asText(
            "Could you tell me more about your campaign goals?"
        ));
    }

    /**
     * Sealed result type for the agentic decision.
     */
    public sealed interface AgentDecision permits AgentDecision.Clarify, AgentDecision.Search {

        record Clarify(String question) implements AgentDecision {}
        record Search(RecommendationRequestDTO request) implements AgentDecision {}

        static AgentDecision clarify(String question) { return new Clarify(question); }
        static AgentDecision search(RecommendationRequestDTO req) { return new Search(req); }
    }
}
