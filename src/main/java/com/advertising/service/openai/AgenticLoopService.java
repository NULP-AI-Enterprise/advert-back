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
import java.util.stream.Collectors;

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

        ═══ STRICT RULES — follow every one ═══

        DISMISSAL: If the user says "does not matter", "doesn't matter", "any", "not important",
        "skip", "no preference", "whatever", "no budget", or otherwise refuses to provide a field —
        accept null/any for that field, NEVER ask about it again, and move forward.

        REPEAT SIGNAL: If the user says "already provided", "I told you", "you already asked",
        "see above", or similar — look through the full conversation history above to extract
        the answer instead of asking again.

        BUDGET CURRENCY: Accept budget in any form: "500 eur", "€500", "10000 грн", "$2000",
        "2k usd". Convert to approximate USD if needed (1 EUR ≈ 1.1 USD, 1 UAH ≈ 0.024 USD).
        "no budget", "not sure", "flexible" = null.

        MAX CLARIFY: If 3 or more assistant messages already appear in the conversation history,
        STOP clarifying. Set action="search" using the best available information and sensible
        defaults for any still-missing fields. Never ask a 4th clarifying question.

        LARGE BRIEF: If the user's very first message already contains a product/service AND
        at least two of (target audience, location, campaign objective, budget) — even described
        in natural language — set action="search" immediately, do not ask anything.

        EAGER SEARCH: Proceed to search as soon as you know:
        - The product/service being advertised (REQUIRED)
        - ANY TWO of: target audience, location/region, campaign objective, budget
        Use "awareness" as default objective and null for missing optional fields.

        ═══ REASONING STEPS ═══

        Before choosing an action, work through what you already know from the FULL conversation:
        1. Product/service — check every user message, even early ones.
        2. Target audience — age, interests, demographics; vague hints count.
        3. Campaign objective — explicit or strongly implied.
        4. Region/location — anywhere in history, including device context.
        5. Budget — any currency, any format.
        6. How many assistant messages are already in the history? (→ MAX CLARIFY rule)

        ═══ ACTIONS ═══

        ACTION "clarify" — ask ONE question for the single most critical missing field.
                           Only allowed if fewer than 3 assistant messages exist in history.
        ACTION "search"  — enough context collected; output best-effort search parameters.
        ACTION "plan"    — user explicitly asked to create a marketing plan.

        For "search" also generate 3 short follow-up suggestions. These MUST be concrete
        search-refinement commands the user can click to narrow or adjust the results —
        NOT generic marketing advice. Good examples:
          "Show only national-reach media"
          "Filter by video format only"
          "Create marketing plan"
          "Show top 5 results only"
          "Search in Lviv region instead"
          "Focus on Business category only"
          "Increase to 15 results"
        Bad examples (never generate these):
          "Explore creative strategies"
          "Consider additional regions"
          "Identify key metrics for success"
          "Adjust campaign timing"

        Respond ONLY in valid JSON — no markdown, no prose outside JSON:
        {
          "reasoning": "<2-3 sentences: what you found in history, clarify count, why this action>",
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
          "suggestions": ["<concrete refinement command>", "<concrete refinement command>", "<concrete refinement command>"]
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

        long assistantTurns = history.stream()
            .filter(m -> "assistant".equalsIgnoreCase(m.getRole().name()))
            .count();
        if (assistantTurns >= 3) {
            systemContent.append("\n\n⚠️ MANDATORY OVERRIDE: ")
                .append(assistantTurns)
                .append(" clarifying messages have already been sent. ")
                .append("You MUST set action=\"search\" now. ")
                .append("Extract all available facts from the conversation history above. ")
                .append("Use 'awareness' as default objective if not stated. ")
                .append("Do NOT ask any further questions.");
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
