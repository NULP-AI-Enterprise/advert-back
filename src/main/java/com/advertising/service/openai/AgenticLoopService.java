package com.advertising.service.openai;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.ChatMessage;
import com.advertising.service.chat.ChatHistoryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final ObjectMapper objectMapper;

    @Value("${app.recommendation.max-results}")
    private int defaultMaxResults;

    @Value("${app.recommendation.context-window:20}")
    private int contextWindow;

    // ── JSON Schema for Structured Outputs ────────────────────────────────────
    // Strict mode: all fields required, nullable via ["type","null"], no extra keys.
    // This replaces the JSON description block that was previously in the prompt text.

    private static final String ROUTER_SCHEMA_JSON = """
        {
          "type": "object",
          "properties": {
            "reasoning":    { "type": "string" },
            "action":       { "type": "string" },
            "question":     { "type": ["string", "null"] },
            "search_params": {
              "anyOf": [
                {
                  "type": "object",
                  "properties": {
                    "categories":                   { "type": "array",  "items": { "type": "string" } },
                    "keywords":                     { "type": "array",  "items": { "type": "string" } },
                    "target_audience_description":  { "type": ["string", "null"] },
                    "age_range": {
                      "anyOf": [
                        {
                          "type": "object",
                          "properties": {
                            "min": { "type": ["integer", "null"] },
                            "max": { "type": ["integer", "null"] }
                          },
                          "required": ["min", "max"],
                          "additionalProperties": false
                        },
                        { "type": "null" }
                      ]
                    },
                    "budget_usd":           { "type": ["number",  "null"] },
                    "campaign_objective":   { "type": ["string",  "null"] },
                    "format_preference":    { "type": ["string",  "null"] },
                    "event_date":           { "type": ["string",  "null"] },
                    "region":               { "type": ["string",  "null"] },
                    "country":              { "type": ["string",  "null"] },
                    "max_results":          { "type": "integer" }
                  },
                  "required": [
                    "categories","keywords","target_audience_description","age_range",
                    "budget_usd","campaign_objective","format_preference","event_date",
                    "region","country","max_results"
                  ],
                  "additionalProperties": false
                },
                { "type": "null" }
              ]
            },
            "suggestions": { "type": "array", "items": { "type": "string" } }
          },
          "required": ["reasoning","action","question","search_params","suggestions"],
          "additionalProperties": false
        }
        """;

    private JsonNode routerSchema;

    @jakarta.annotation.PostConstruct
    void init() {
        try {
            routerSchema = objectMapper.readTree(ROUTER_SCHEMA_JSON);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse router JSON schema", e);
        }
    }

    // ── Prompt ─────────────────────────────────────────────────────────────────

    private static final String ROUTER_SYSTEM_PROMPT = """
        You are an AI assistant helping users find the best media placements for their advertising campaigns.

        ═══ STRICT RULES ═══

        DISMISSAL: Treat user indifference expressions ("any", "skip", "no preference",
        "does not matter", "no budget", "flexible", "not sure") as explicit NULL values
        for that field. Set it to null and proceed — never ask about it again.

        REPEAT SIGNAL: If the user says "already provided", "I told you", "you already asked",
        or similar — look through the full conversation history to extract the answer.

        BUDGET CURRENCY: Accept any format: "500 eur", "€500", "10000 грн", "$2000", "2k usd".
        Convert to USD (1 EUR ≈ 1.1 USD, 1 UAH ≈ 0.024 USD). Indifference expressions → null.

        MAX CLARIFY: You have been given CURRENT_CLARIFY_COUNT (the number of assistant
        clarifying turns already sent). If CURRENT_CLARIFY_COUNT >= 3 → set action="search"
        immediately using best available information. Never send a 4th clarifying question.

        LARGE BRIEF: If the user's first message contains a product/service PLUS at least
        two of (audience, location, objective, budget, format) — set action="search" at once.

        EAGER SEARCH: Proceed to search as soon as you know:
        - The product/service being advertised (REQUIRED)
        - ANY TWO of: audience, location, objective, budget, format
        Default objective: "awareness". Missing optional fields → null.

        CLARIFY PRIORITY — when you must ask exactly ONE question:
        1. Budget AND format both unknown → combine: "What's your budget and preferred format
           — Article, Press Release, Paid news, or Video?"
        2. Only budget unknown → ask budget.
        3. Only format unknown → ask format.
        4. Otherwise ask about the single most impactful missing field.

        ═══ REASONING STEPS ═══

        Check the FULL conversation before deciding:
        1. Product/service — scan every user message.
        2. Audience — age, interests, demographics; vague hints count.
        3. Objective — explicit or strongly implied.
        4. Region/location — any message, including device context.
        5. Budget — any currency or format.
        6. Format preference — Article / Press Release / Paid news / Video.
        7. Event date — extract if this is a timed event.
        8. CURRENT_CLARIFY_COUNT — read the injected value, do not count yourself.

        ═══ ACTIONS ═══

        action="clarify" — ONE question covering 1-2 critical missing fields.
                           Only if CURRENT_CLARIFY_COUNT < 3.
        action="search"  — output structured search parameters.
        action="plan"    — user explicitly asked for a marketing plan.

        For action="search" return 3 suggestions — CONCRETE refinement commands, e.g.:
          "Filter by Article format only"
          "Filter by Video format only"
          "Show only national-reach media"
          "Focus on Technology category only"
          "Set budget to $500"
          "Search in Kyiv region instead"
          "Increase to 15 results"
          "Create marketing plan"
        Never suggest vague advice like "Explore creative strategies".

        ═══ CATEGORY CLASSIFICATION ═══

        Canonical values only:
        News | Business | Technology | Sports | Fashion | Agriculture | Video | Entertainment | Science | Politics

        Map broadly:
          fintech/crypto/blockchain/web3/SaaS/AI/drones/hardware → Technology
          finance/investment/insurance/real estate/B2B           → Business
          health/medicine/biotech                                 → Science
          politics/government/NGO                                 → Politics
        Return up to 3 best-matching categories.

        ═══ KEYWORD RULES ═══

        Extract 5-8 keywords for full-text search against outlet titles, URLs, descriptions, tags.
        1. Minimum 4 characters — never "AI", "IT", "ML", "AR", "VR", "B2B".
        2. Spell out: "AI" → "artificial intelligence" + "machine learning";
           "IT" → "technology" + "digital" + "software"; "ML" → "machine learning".
        3. Domain-friendly terms (likely in site names/URLs):
           Technology → "tech", "digital", "software", "startup"
           Business   → "business", "finance", "economic"
           Science    → "science", "research"
        4. Specific + synonyms: "drones" → also "drone", "aviation", "aerospace", "UAV".
        5. Prefer longer specific terms over short generic ones.

        ═══ FORMAT EXTRACTION ═══

        Map to: "Article" | "Press Release" | "Paid news" | "Video" | null (no preference).
        "стаття"/"article"/"post" → "Article"
        "відео"/"video"          → "Video"
        "прес-реліз"             → "Press Release"
        "новина"/"news"          → "Paid news"

        ═══ EVENT DATE ═══

        If the campaign is for a timed event, extract event_date as YYYY-MM-DD.
        Leave null if no date mentioned.

        ═══ GEO ═══

        region: most specific location (city/district). country: the country. Both nullable.
        """;

    // ── Public API ─────────────────────────────────────────────────────────────

    public Mono<AgentDecision> decide(String sessionId, String userMessage,
                                      String deviceLocation, String deviceLanguage,
                                      String previousContext) {
        log.info("[Agentic] decide() session={}", sessionId);
        return chatHistoryService.getRecentHistory(sessionId, contextWindow)
            .flatMap(history -> {
                long clarifyCount = history.stream()
                    .filter(m -> "assistant".equalsIgnoreCase(m.getRole().name()))
                    .count();
                log.info("[Agentic] history={} msgs, clarifyCount={}, calling OpenAI", history.size(), clarifyCount);
                List<Map<String, String>> messages = buildMessageList(
                    history, userMessage, deviceLocation, deviceLanguage, previousContext, clarifyCount);
                return openAIService.chatCompletionStructured(messages, "router_response", routerSchema);
            })
            .map(json -> {
                String reasoning = json.path("reasoning").asText("");
                if (!reasoning.isBlank()) log.info("[Agentic] reasoning: {}", reasoning);
                return parseDecision(json, sessionId);
            })
            .doOnError(e -> log.error("[Agentic] OpenAI call failed: {}", e.getMessage(), e));
    }

    public Mono<AgentDecision> decide(String sessionId, String userMessage) {
        return decide(sessionId, userMessage, null, null, null);
    }

    // ── Internal ────────────────────────────────────────────────────────────────

    private List<Map<String, String>> buildMessageList(
            List<ChatMessage> history, String newUserMessage,
            String deviceLocation, String deviceLanguage,
            String previousContext, long clarifyCount) {

        StringBuilder systemContent = new StringBuilder(ROUTER_SYSTEM_PROMPT);

        // Inject count as explicit variable — model reads it, never counts itself
        systemContent.append("\n\nCURRENT_CLARIFY_COUNT: ").append(clarifyCount);
        if (clarifyCount >= 3) {
            systemContent.append(" ← LIMIT REACHED. You MUST set action=\"search\" now.");
        }

        if ((deviceLocation != null && !deviceLocation.isBlank())
                || (deviceLanguage != null && !deviceLanguage.isBlank())) {
            systemContent.append("\n\nDevice context (fallback if user hasn't specified):");
            if (deviceLocation != null && !deviceLocation.isBlank())
                systemContent.append("\n- Location: ").append(deviceLocation);
            if (deviceLanguage != null && !deviceLanguage.isBlank())
                systemContent.append("\n- Language: ").append(deviceLanguage);
        }

        if (previousContext != null && !previousContext.isBlank()) {
            systemContent.append("\n\nPrevious search context (avoid repeating results):\n")
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

            if (params.has("categories") && !params.path("categories").isNull()) {
                List<String> cats = new ArrayList<>();
                params.path("categories").forEach(n -> cats.add(n.asText()));
                builder.categories(cats);
            }

            if (params.has("keywords") && !params.path("keywords").isNull()) {
                List<String> kws = new ArrayList<>();
                params.path("keywords").forEach(n -> kws.add(n.asText()));
                kws.removeIf(k -> k == null || k.trim().length() < 4);
                if (!kws.isEmpty()) builder.keywords(kws);
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

            JsonNode budgetNode = params.path("budget_usd");
            if (!budgetNode.isNull() && !budgetNode.isMissingNode())
                builder.budgetUsd(budgetNode.asDouble());

            String objective = params.path("campaign_objective").asText(null);
            if (objective != null && !objective.isBlank()) builder.campaignObjective(objective);

            String region = params.path("region").asText(null);
            if (region != null && !region.isBlank() && !"null".equals(region)) builder.region(region);

            String country = params.path("country").asText(null);
            if (country != null && !country.isBlank() && !"null".equals(country)) builder.country(country);

            String format = params.path("format_preference").asText(null);
            if (format != null && !format.isBlank() && !"null".equalsIgnoreCase(format)
                    && !"any".equalsIgnoreCase(format))
                builder.formatPreference(format);

            String eventDate = params.path("event_date").asText(null);
            if (eventDate != null && !eventDate.isBlank() && !"null".equals(eventDate))
                builder.eventDate(eventDate);

            int maxResults = params.path("max_results").asInt(defaultMaxResults);
            builder.maxResults(maxResults > 0 ? maxResults : defaultMaxResults);

            return AgentDecision.search(builder.build(), suggestions);
        }

        return AgentDecision.clarify(json.path("question").asText(
            "Could you tell me more about your campaign goals?"
        ));
    }

    // ── Decision type ───────────────────────────────────────────────────────────

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
