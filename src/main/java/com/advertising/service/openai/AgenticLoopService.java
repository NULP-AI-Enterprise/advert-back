package com.advertising.service.openai;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.ChatMessage;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.service.chat.ChatHistoryService;
import com.advertising.service.debug.DebugEvents;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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

        BUDGET CURRENCY: Accept any format: "500 eur", "€500", "$2000", "2k usd", "£1500",
        "10000 грн", "5000 zł", "50000 ₹", "200000 ¥".
        Convert to USD using approximate current exchange rates (use your knowledge of current rates
        for any currency). Indifference expressions → null.

        MAX CLARIFY: You have been given CURRENT_CLARIFY_COUNT and RESULTS_SHOWN.
        - If RESULTS_SHOWN=false AND CURRENT_CLARIFY_COUNT >= 3 → set action="search" immediately.
        - If RESULTS_SHOWN=true → you MAY ask ONE targeted refinement question (action="clarify")
          when the user's message is vague ("find more", "show other options", "different media",
          "something better"). This is a POST-RESULTS collaboration, not a pre-search clarification.
          If the user sends a clear update (budget/format/category/region) → action="search".

        ACTIVE PARAMS: When RESULTS_SHOWN=true, an "ACTIVE SEARCH PARAMETERS" block is injected
        above the conversation. These ALWAYS take priority over earlier mentions in history.
        When user says "find more", "show more results", "more media" → keep ALL active params,
        just increase max_results by 5 (or to 15 if currently ≤10).

        LARGE BRIEF: If the user's first message contains a product/service PLUS at least
        two of (audience, location, objective, budget, format) — set action="search" at once.

        EAGER SEARCH: Proceed to search as soon as you know:
        - The product/service being advertised (REQUIRED)
        - ANY TWO of: audience, location, objective, budget, format
        Default objective: "awareness". Missing optional fields → null.

        CLARIFY PRIORITY — when you must ask ONE question (pre-search OR post-results refinement):
        Pre-search:
          1. Budget AND format both unknown → ask both together.
          2. Only budget unknown → ask budget.
          3. Only format unknown → ask format.
          4. Otherwise ask the single most impactful missing field.
        Post-results (when RESULTS_SHOWN=true, user is vague):
          Ask ONE specific question that would most improve the results, e.g.:
          "Are you targeting hiring (job candidates) or sales (B2B clients)?",
          "What specific services do you offer — web dev, mobile apps, AI?",
          "Any preferred regions or local-only media?"

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
          "Search in a different region"
          "Increase to 15 results"
          "Create marketing plan"
        Never suggest vague advice like "Explore creative strategies".

        ═══ CATEGORY CLASSIFICATION ═══

        Canonical values only:
        News | Business | Technology | Sports | Fashion | Agriculture | Video | Entertainment | Science | Health | Politics

        Map broadly:
          fintech/crypto/blockchain/web3/SaaS/AI/drones/hardware → Technology
          finance/investment/insurance/real estate/B2B           → Business
          health/medicine/pharma/wellness/fitness/medical        → Health
          biotech/life-sciences                                   → Science
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

        ═══ LANGUAGE DETECTION ═══

        If the user writes in a non-English language AND no country has been explicitly set,
        infer the most likely country as a soft default:
          Ukrainian Cyrillic (letters і, ї, є, щ or words "для", "або", "що", "це", "та") → "Ukraine"
          Russian Cyrillic (ё, ъ, ы dominant, no і/ї/є) → "Russia"
          French → "France"
          German → "Germany"
          Spanish → "Spain" (if Latin American terms appear, ask to clarify)
          Polish → "Poland"
          Turkish → "Turkey"
          Arabic → ask to clarify (many countries share Arabic)
          Other languages → use your knowledge to infer the most likely country
        Always a soft inference — override immediately if the user specifies otherwise.
        Never ask the user to confirm their country when language inference is sufficient.
        """;

    // ── Public API ─────────────────────────────────────────────────────────────

    public Mono<AgentDecision> decide(String sessionId, String userMessage,
                                      String deviceLocation, String deviceLanguage,
                                      String previousContext,
                                      Consumer<WebSocketMessage> debug) {
        log.info("[Chat] ── STAGE 1/3: ROUTER LLM ────────────────────────");
        log.info("[Agentic] decide() session={}", sessionId);
        return chatHistoryService.getRecentHistory(sessionId, contextWindow)
            .flatMap(history -> {
                long clarifyCount = history.stream()
                    .filter(m -> "assistant".equalsIgnoreCase(m.getRole().name()))
                    .count();
                boolean resultsShown = previousContext != null && !previousContext.isBlank();
                log.info("[Agentic] history={} msgs, clarifyCount={}, calling OpenAI", history.size(), clarifyCount);

                // Debug: what we're sending to the router LLM
                Map<String, Object> inputData = new LinkedHashMap<>();
                inputData.put("history_msgs", history.size());
                inputData.put("clarify_count", clarifyCount);
                inputData.put("results_shown", resultsShown);
                inputData.put("user_message", userMessage);
                if (deviceLanguage != null) inputData.put("device_language", deviceLanguage);
                if (deviceLocation != null) inputData.put("device_location", deviceLocation);
                if (resultsShown) inputData.put("active_params_preview",
                    previousContext.lines().limit(5).collect(Collectors.joining("\n")));
                DebugEvents.emit(debug, sessionId, "router", "llm_input",
                    "Router → LLM (preparing request)", inputData);

                List<Map<String, String>> messages = buildMessageList(
                    history, userMessage, deviceLocation, deviceLanguage, previousContext, clarifyCount);

                log.info("[Agentic] → router LLM total_messages={} history_turns={} clarify_count={} results_shown={}",
                    messages.size(), history.size(), clarifyCount, resultsShown);
                log.debug("[Agentic] full message list ({} messages):", messages.size());
                for (int i = 0; i < messages.size(); i++) {
                    String role    = messages.get(i).getOrDefault("role", "?");
                    String content = messages.get(i).getOrDefault("content", "");
                    log.debug("[Agentic]   [{}] role={} chars={} preview='{}'",
                        i, role, content.length(),
                        content.length() > 250 ? content.substring(0, 250) + "…" : content);
                }

                return openAIService.chatCompletionStructured(messages, "router_response", routerSchema);
            })
            .map(json -> {
                String reasoning = json.path("reasoning").asText("");
                String action    = json.path("action").asText("clarify");
                if (!reasoning.isBlank()) log.info("[Agentic] reasoning: {}", reasoning);

                // Debug: what the router LLM decided
                Map<String, Object> outputData = new LinkedHashMap<>();
                outputData.put("action", action);
                outputData.put("reasoning", reasoning);
                if ("clarify".equals(action)) {
                    outputData.put("question", json.path("question").asText(""));
                }
                if ("search".equals(action) && !json.path("search_params").isNull()) {
                    JsonNode sp = json.path("search_params");
                    Map<String, Object> params = new LinkedHashMap<>();
                    if (sp.has("categories"))  params.put("categories", sp.path("categories").toString());
                    if (sp.has("keywords"))     params.put("keywords", sp.path("keywords").toString());
                    if (!sp.path("region").isNull()) params.put("region", sp.path("region").asText());
                    if (!sp.path("country").isNull()) params.put("country", sp.path("country").asText());
                    if (!sp.path("budget_usd").isNull()) params.put("budget_usd", sp.path("budget_usd").asText());
                    if (!sp.path("format_preference").isNull()) params.put("format", sp.path("format_preference").asText());
                    if (sp.has("max_results")) params.put("max_results", sp.path("max_results").asInt());
                    outputData.put("search_params", params);
                }
                List<String> sugg = new ArrayList<>();
                json.path("suggestions").forEach(n -> sugg.add(n.asText()));
                if (!sugg.isEmpty()) outputData.put("suggestions", sugg);

                DebugEvents.emit(debug, sessionId, "router", "llm_output",
                    "LLM Decision: " + action.toUpperCase(), outputData);

                return parseDecision(json, sessionId);
            })
            .doOnError(e -> {
                log.error("[Agentic] OpenAI call failed: {}", e.getMessage(), e);
                DebugEvents.emit(debug, sessionId, "router", "llm_error",
                    "Router LLM Error", Map.of("error", e.getMessage()));
            });
    }

    public Mono<AgentDecision> decide(String sessionId, String userMessage,
                                      String deviceLocation, String deviceLanguage,
                                      String previousContext) {
        return decide(sessionId, userMessage, deviceLocation, deviceLanguage, previousContext, DebugEvents.NOOP);
    }

    public Mono<AgentDecision> decide(String sessionId, String userMessage) {
        return decide(sessionId, userMessage, null, null, null, DebugEvents.NOOP);
    }

    // ── Internal ────────────────────────────────────────────────────────────────

    private List<Map<String, String>> buildMessageList(
            List<ChatMessage> history, String newUserMessage,
            String deviceLocation, String deviceLanguage,
            String previousContext, long clarifyCount) {

        StringBuilder systemContent = new StringBuilder(ROUTER_SYSTEM_PROMPT);

        boolean resultsShown = previousContext != null && !previousContext.isBlank();

        // Inject count as explicit variable — model reads it, never counts itself
        systemContent.append("\n\nCURRENT_CLARIFY_COUNT: ").append(clarifyCount);
        systemContent.append("\nRESULTS_SHOWN: ").append(resultsShown);
        if (clarifyCount >= 3 && !resultsShown) {
            systemContent.append(" ← PRE-SEARCH LIMIT REACHED. Set action=\"search\" now.");
        }

        if ((deviceLocation != null && !deviceLocation.isBlank())
                || (deviceLanguage != null && !deviceLanguage.isBlank())) {
            systemContent.append("\n\nDevice context (fallback if user hasn't specified):");
            if (deviceLocation != null && !deviceLocation.isBlank())
                systemContent.append("\n- Location: ").append(deviceLocation);
            if (deviceLanguage != null && !deviceLanguage.isBlank())
                systemContent.append("\n- Language: ").append(deviceLanguage);
        }

        if (resultsShown) {
            // Frame as authoritative active params — LLM must use these as base,
            // not fall back to earlier mentions in conversation history.
            systemContent.append("\n\n⚠️ ACTIVE SEARCH PARAMETERS — these OVERRIDE anything in conversation history.\n")
                         .append("Use them as defaults for the next search. Only change what the user explicitly requests:\n")
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

            RecommendationRequestDTO req = builder.build();
            log.info("[Agentic] ══ ROUTER → SEARCH ══ " +
                "categories={} keywords={} country='{}' region='{}' budget={} format='{}' audience='{}' maxResults={}",
                req.getCategories(),
                req.getKeywords(),
                req.getCountry(),
                req.getRegion(),
                req.getBudgetUsd() != null ? "$" + req.getBudgetUsd() : "null",
                req.getFormatPreference(),
                req.getTargetAudienceDescription() != null
                    ? (req.getTargetAudienceDescription().length() > 80
                        ? req.getTargetAudienceDescription().substring(0, 80) + "…"
                        : req.getTargetAudienceDescription())
                    : "null",
                req.getMaxResults());
            return AgentDecision.search(req, suggestions);
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
