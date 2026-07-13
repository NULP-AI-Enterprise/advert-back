package com.advertising.service.enrichment;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.dto.RecommendationResponseDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.service.debug.DebugEvents;
import com.advertising.service.openai.OpenAIService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;

/**
 * Takes raw SQL candidates and uses GPT to produce enriched recommendations
 * with reasoning grounded in REAL data: cost_usd, similarweb_visits, ahrefs_dr,
 * moz_da, format_type, language, and content restrictions from PRNEW CSV.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EnrichmentMechanismService {

    private final OpenAIService openAIService;
    private final ObjectMapper objectMapper;

    // ── JSON Schema for Structured Outputs ─────────────────────────────────────

    private static final String ENRICHMENT_SCHEMA_JSON = """
        {
          "type": "object",
          "properties": {
            "reasoning": { "type": "string" },
            "recommendations": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "media_item_id":   { "type": "string" },
                  "match_score":     { "type": "integer" },
                  "match_reason":    { "type": "string" },
                  "suggested_format":{ "type": "string" },
                  "estimated_reach": { "type": "string" },
                  "budget_fit":      { "type": "string" }
                },
                "required": ["media_item_id","match_score","match_reason","suggested_format","estimated_reach","budget_fit"],
                "additionalProperties": false
              }
            }
          },
          "required": ["reasoning","recommendations"],
          "additionalProperties": false
        }
        """;

    private JsonNode enrichmentSchema;

    @jakarta.annotation.PostConstruct
    void init() {
        try {
            enrichmentSchema = objectMapper.readTree(ENRICHMENT_SCHEMA_JSON);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse enrichment JSON schema", e);
        }
    }

    // ── System Prompt ───────────────────────────────────────────────────────────

    private static final String SYSTEM_PROMPT = """
        You are a senior media planning strategist with expertise across global markets.
        Evaluate PR placement opportunities against a campaign brief and return ONLY the
        best matches — not every candidate. Be decisive: if an outlet doesn't fit, exclude it.

        You have access to REAL data for each outlet:
        - cost_usd: actual placement price from the PR marketplace
        - similarweb_visits: verified monthly traffic
        - ahrefs_dr / moz_da: SEO authority scores
        - format_type: placement format (Article, Press Release, Paid news, Video, etc.)
        - language: publication language
        - lead_time_hours: advance booking required (pre-computed feasibility also provided)
        - restrictions: explicit content rules (see below)

        ═══ SCORING ═══

        80-100: Excellent — cost, audience, category, format, restrictions all align
        60-79:  Good — most factors align, minor gaps
        40-59:  Partial — useful but not ideal
        30-39:  Weak — include only when the pool is small
        < 30:   Poor — EXCLUDE (omit from response entirely)

        Exclusion threshold is 30. A missed relevant result is worse than one extra mediocre one.

        MINIMUM RESULTS RULE: Always return at least 5 results, even if some score below 40.
        If fewer than 5 candidates score ≥ 30, include the next-best candidates until you
        have 5, noting in match_reason "included as best available option" for scores < 30.
        Return at most 10 items sorted by score descending.

        ═══ BUDGET ═══

        Use exact cost_usd — never vague tiers.
        NEVER exclude an outlet solely because cost exceeds budget.
        Always include and flag — let the user decide. Apply score penalties:
        - cost_usd ≤ budget              → no penalty; note in budget_fit
        - cost_usd > budget × 0.5        → reduce score by 10, note in budget_fit
        - cost_usd > budget              → reduce score by 20, note "Over budget by $X" in budget_fit
        - cost_usd > budget × 2          → reduce score by 35, note "Significantly over budget"
        - No budget specified            → no penalty; mention cost in reasoning

        ═══ FORMAT MATCHING ═══

        If format_preference is set in the brief:
        - Outlet format_type matches preference → boost +10
        - No preference → suggest best format by objective:
          leads/conversions → Article or Paid news
          awareness → Press Release or Article
          engagement → Video if available, else Article

        ═══ LEAD TIME FEASIBILITY ═══

        Each outlet now includes a pre-computed "Booking feasible" flag.
        - feasible=YES → mention it as a positive signal, boost +5
        - feasible=NO  → flag in match_reason as "booking may be too late"; reduce score by 15

        ═══ RESTRICTIONS ═══

        The restrictions map contains explicit content rules. Follow this decision tree exactly:

        1. Key is ABSENT or restrictions is empty → content is PERMITTED. Do NOT exclude.
        2. Key is present with value TRUE  → content EXPLICITLY ALLOWED. Boost score +5.
        3. Key is present with value FALSE → content FORBIDDEN. EXCLUDE the outlet.

        Read each restriction key carefully. If the campaign topic does not appear in
        restrictions at all, the outlet is safe to include.

        ═══ PRE-ENRICHMENT HANDLING ═══

        Some outlets have no description/tags yet. Infer category from URL domain signals:
          Domain contains "tech","digit","soft","start","code","dev","data","hack","cyber",
          "web3","crypto","ai","ml","drone","robot"   → Technology
          Domain contains "biz","finance","invest","bank","econom","market","trade"
                                                       → Business
          Domain contains "health","med","pharm","clinic","doctor","hospital"
                                                       → Health
          Domain contains "sport","football","soccer","basket","tennis","boxing"
                                                       → Sports
          Domain contains "agro","farm","agri","grain","land" → Agriculture
          Domain contains "fashion","style","beauty","mode"   → Fashion
          Domain contains "entertain","culture","kino","film" → Entertainment
          All other unenriched domains → News (default)

        Traffic tiers for unenriched outlets:
          similarweb_visits > 1M   → base score 60
          similarweb_visits 100K–1M → base score 55
          All other unenriched     → base score 50
        Never score below 30 solely because description is missing.

        ═══ AUDIENCE DATA USAGE ═══

        Each outlet's "Audience:" field contains structured data: age_range, interests,
        demographics.geo, and gender_split. When the campaign brief includes age range,
        target audience, or region — you MUST use this data:

        1. Age overlap: if outlet audience.age_range does NOT overlap campaign age range
           → reduce score by 10 and note "audience age mismatch" in match_reason.
        2. Interest overlap: if audience.interests contains 2+ keywords matching the campaign
           product/service → boost score by 5 and explicitly cite those interests.
        3. Geo specificity: cite demographics.geo in estimated_reach when it names a specific
           city or region (more precise than just the country name).

        Example match_reason with audience data:
          "Targets 25-40 tech professionals (aligns with campaign's 28-45 range),
           with stated interests in machine learning and startups — strong signal match.
           890K monthly visits, DR=61. $220 of $3000 budget — 13.6× per month."

        ═══ REQUIRED OUTPUT PER RECOMMENDATION ═══

        match_reason: 2-3 sentences citing SPECIFIC numbers. Must include:
          • Traffic: e.g. "1.6M monthly visits, DR=77"
          • Cost vs budget: e.g. "$130 of $2000 — 6.5%"
          • Why audience/topic matches
          • If event_date given: mention booking feasibility
        suggested_format: best format from available format_type values
        estimated_reach: concrete, e.g. "~1.6M monthly readers, national reach" or "~250K readers, London metro area"
        budget_fit: short line, e.g. "$130 of $2000 — fits 15× per month"

        Top-level `reasoning` (3-5 sentences):
          • What the budget allows
          • Why selected outlets match audience and objective
          • Format strategy rationale
          • Trade-offs, timing risks, or gaps

        Sort recommendations by match_score descending. Return at most 10 items.
        """;

    // ── Public API ──────────────────────────────────────────────────────────────

    public Mono<RecommendationResponseDTO> enrich(
            List<MediaItem> rawCandidates,
            RecommendationRequestDTO request,
            Consumer<WebSocketMessage> debug) {

        String sid = request.getSessionId();

        if (rawCandidates.isEmpty()) {
            log.warn("[Enrichment] no candidates for session={}", sid);
            DebugEvents.emit(debug, sid, "enrichment", "no_candidates",
                "No Candidates — returning empty result", Map.of());
            return Mono.just(RecommendationResponseDTO.builder()
                .sessionId(sid)
                .recommendations(List.of())
                .build());
        }

        log.info("[Enrichment] enriching {} candidates for session={}", rawCandidates.size(), sid);

        // Emit what we're sending to the scoring LLM
        Map<String, Object> inputData = new LinkedHashMap<>();
        inputData.put("candidate_count", rawCandidates.size());
        inputData.put("categories", request.getCategories() != null ? request.getCategories() : List.of());
        if (request.getBudgetUsd() != null) inputData.put("budget_usd", request.getBudgetUsd());
        if (request.getTargetAudienceDescription() != null) inputData.put("audience", request.getTargetAudienceDescription());
        if (request.getFormatPreference() != null) inputData.put("format_preference", request.getFormatPreference());
        if (request.getRegion() != null) inputData.put("region", request.getRegion());
        inputData.put("candidate_titles", rawCandidates.stream().map(MediaItem::getTitle).toList());
        DebugEvents.emit(debug, sid, "enrichment", "llm_input",
            "Scoring LLM Input (" + rawCandidates.size() + " candidates)", inputData);

        EventDateContext eventCtx = parseEventDate(request.getEventDate());

        String userPrompt = buildUserPrompt(rawCandidates, request, eventCtx);
        int totalChars = SYSTEM_PROMPT.length() + userPrompt.length();
        int estTokens  = totalChars / 4;
        int avgCharsPerItem = userPrompt.length() / Math.max(1, rawCandidates.size());
        log.info("[Enrichment] ══ TOKEN ESTIMATE ══ ~{} tokens ({} items × ~{} chars/item | system={} chars)",
            estTokens, rawCandidates.size(), avgCharsPerItem, SYSTEM_PROMPT.length());
        log.info("[Enrichment] campaign brief preview: '{}'",
            userPrompt.length() > 400 ? userPrompt.substring(0, 400) + "…" : userPrompt);
        log.debug("[Enrichment] full user_prompt chars={}", userPrompt.length());

        List<Map<String, String>> messages = List.of(
            Map.of("role", "system", "content", SYSTEM_PROMPT),
            Map.of("role", "user",   "content", userPrompt)
        );

        log.info("[Enrichment] → scoring LLM system_chars={} user_chars={} est_tokens=~{} schema=enrichment_response",
            SYSTEM_PROMPT.length(), userPrompt.length(), estTokens);

        return openAIService.chatCompletionStructured(messages, "enrichment_response", enrichmentSchema)
            .doOnNext(json -> {
                String reasoning = json.path("reasoning").asText("");
                List<Map<String, Object>> scores = new ArrayList<>();
                json.path("recommendations").forEach(rec -> {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    String idStr = rec.path("media_item_id").asText(null);
                    entry.put("id", idStr != null ? idStr : "");
                    entry.put("score", rec.path("match_score").asInt(0));
                    entry.put("format", rec.path("suggested_format").asText(""));
                    entry.put("reason", rec.path("match_reason").asText(""));
                    if (idStr != null) {
                        rawCandidates.stream()
                            .filter(m -> m.getId().toString().equals(idStr))
                            .findFirst()
                            .ifPresent(m -> entry.put("title", m.getTitle()));
                    }
                    scores.add(entry);
                });

                // Score distribution — how many in each tier
                long excellent = scores.stream().filter(s -> (Integer) s.get("score") >= 80).count();
                long good      = scores.stream().filter(s -> { int sc = (Integer) s.get("score"); return sc >= 60 && sc < 80; }).count();
                long partial   = scores.stream().filter(s -> { int sc = (Integer) s.get("score"); return sc >= 30 && sc < 60; }).count();
                int excluded   = rawCandidates.size() - scores.size();

                log.info("[Enrichment] ← LLM returned {} recommendations from {} candidates " +
                    "(excellent≥80: {} good≥60: {} partial≥30: {} excluded<30: {})",
                    scores.size(), rawCandidates.size(), excellent, good, partial, excluded);

                String scoresLine = scores.stream()
                    .map(s -> String.valueOf(s.get("score")))
                    .collect(java.util.stream.Collectors.joining(", "));
                log.info("[Enrichment] ══ SCORES ══ [{}]", scoresLine);

                if (!scores.isEmpty()) {
                    Map<String, Object> top = scores.get(0);
                    String topReason = top.get("reason") instanceof String r
                        ? (r.length() > 160 ? r.substring(0, 160) + "…" : r) : "";
                    log.info("[Enrichment] top match: '{}' score={} reason='{}'",
                        top.getOrDefault("title", top.get("id")), top.get("score"), topReason);
                }

                log.info("[Enrichment] reasoning: '{}'",
                    reasoning.length() > 300 ? reasoning.substring(0, 300) + "…" : reasoning);

                // Per-item detail at DEBUG — visible when needed, not in default INFO view
                scores.forEach(s -> log.debug("[Enrichment] score={} title='{}' format='{}' reason='{}'",
                    s.get("score"),
                    s.getOrDefault("title", s.get("id")),
                    s.get("format"),
                    s.get("reason") instanceof String r
                        ? (r.length() > 120 ? r.substring(0, 120) + "…" : r)
                        : ""));

                Map<String, Object> outputData = new LinkedHashMap<>();
                outputData.put("reasoning", reasoning);
                outputData.put("candidates_in", rawCandidates.size());
                outputData.put("returned_count", scores.size());
                outputData.put("excluded_count", excluded);
                outputData.put("score_distribution",
                    Map.of("excellent_80plus", excellent, "good_60_79", good, "partial_30_59", partial));
                outputData.put("scores", scores);
                DebugEvents.emit(debug, sid, "enrichment", "llm_output",
                    "Scoring LLM Output (" + scores.size() + " of " + rawCandidates.size() + " kept)", outputData);
            })
            .map(json -> buildResponse(json, rawCandidates, sid))
            .doOnNext(resp -> log.info("[Enrichment] DONE: {} final recommendations returned session={}",
                resp.getRecommendations().size(), sid))
            .doOnError(e -> {
                log.error("[Enrichment] LLM failed: {}", e.getMessage(), e);
                DebugEvents.emit(debug, sid, "enrichment", "llm_error",
                    "Scoring LLM Error — using fallback", Map.of("error", e.getMessage()));
            })
            .onErrorReturn(buildFallbackResponse(rawCandidates, sid));
    }

    public Mono<RecommendationResponseDTO> enrich(
            List<MediaItem> rawCandidates,
            RecommendationRequestDTO request) {
        return enrich(rawCandidates, request, DebugEvents.NOOP);
    }

    // ── Prompt building ─────────────────────────────────────────────────────────

    private String buildUserPrompt(List<MediaItem> candidates, RecommendationRequestDTO request,
                                   EventDateContext eventCtx) {
        StringBuilder sb = new StringBuilder();

        sb.append("## Campaign Brief\n");
        if (request.getCategories() != null && !request.getCategories().isEmpty())
            sb.append("- Categories: ").append(String.join(", ", request.getCategories())).append("\n");
        if (request.getTargetAudienceDescription() != null)
            sb.append("- Target Audience: ").append(request.getTargetAudienceDescription()).append("\n");
        if (request.getCampaignObjective() != null)
            sb.append("- Objective: ").append(request.getCampaignObjective()).append("\n");
        if (request.getBudgetUsd() != null)
            sb.append(String.format("- Budget: $%.0f USD%n", request.getBudgetUsd()));
        if (request.getAgeRange() != null)
            sb.append("- Age Range: ").append(request.getAgeRange().getMin())
              .append("-").append(request.getAgeRange().getMax()).append("\n");
        if (request.getRegion() != null)
            sb.append("- Region: ").append(request.getRegion()).append("\n");
        if (request.getFormatPreference() != null)
            sb.append("- Format Preference: ").append(request.getFormatPreference()).append("\n");
        if (eventCtx != null)
            sb.append("- Event Date: ").append(request.getEventDate())
              .append(" (").append(eventCtx.daysUntil()).append(" days from today)\n");
        if (request.getRelaxationNote() != null)
            sb.append("- Note: ").append(request.getRelaxationNote()).append("\n");

        sb.append("\n## Candidate Outlets\n");
        for (MediaItem item : candidates) {
            sb.append("\n### ").append(item.getTitle());
            sb.append("\n- ID: ").append(item.getId());

            if (item.getUrl() != null)
                sb.append("\n- URL: ").append(item.getUrl());

            if (item.getCostUsd() != null)
                sb.append("\n- Cost: $").append(item.getCostUsd()).append(" USD");
            if (item.getFormatType() != null)
                sb.append("\n- Format: ").append(item.getFormatType());
            if (item.getLanguage() != null)
                sb.append("\n- Language: ").append(item.getLanguage());
            if (item.getLeadTimeHours() != null) {
                sb.append("\n- Lead Time: ").append(item.getLeadTimeHours()).append("h");
                // Pre-computed feasibility — LLM reads YES/NO, no arithmetic needed
                if (eventCtx != null) {
                    double leadDays = item.getLeadTimeHours() / 24.0;
                    boolean feasible = leadDays <= eventCtx.daysUntil();
                    sb.append(String.format(" | Booking feasible: %s (%.1f days lead, %d days to event)",
                        feasible ? "YES" : "NO", leadDays, eventCtx.daysUntil()));
                }
            }
            if (item.getHyperlinksType() != null)
                sb.append("\n- Hyperlinks: ").append(item.getHyperlinksType());

            if (item.getSimilarwebVisits() != null)
                sb.append("\n- Traffic: ").append(formatVisits(item.getSimilarwebVisits()))
                  .append(" monthly visits (SimilarWeb)");
            if (item.getAhrefsDr() != null || item.getMozDa() != null || item.getSemrushScore() != null) {
                sb.append("\n- SEO:");
                if (item.getAhrefsDr() != null)    sb.append(" DR=").append(item.getAhrefsDr());
                if (item.getMozDa() != null)        sb.append(" DA=").append(item.getMozDa());
                if (item.getSemrushScore() != null) sb.append(" Semrush=").append(item.getSemrushScore());
            }

            // Restrictions rendered as typed lists — LLM reads declarative facts, not negation logic
            if (item.getRestrictions() != null && !item.getRestrictions().isEmpty()) {
                List<String> allowed    = new ArrayList<>();
                List<String> forbidden  = new ArrayList<>();
                item.getRestrictions().forEach((k, v) -> {
                    if (Boolean.TRUE.equals(v))  allowed.add(k);
                    if (Boolean.FALSE.equals(v)) forbidden.add(k);
                });
                if (!allowed.isEmpty())   sb.append("\n- Explicitly allowed: ").append(String.join(", ", allowed));
                if (!forbidden.isEmpty()) sb.append("\n- FORBIDDEN (exclude if campaign uses): ").append(String.join(", ", forbidden));
            }

            if (item.getCategory() != null)
                sb.append("\n- Category: ").append(item.getCategory());
            if (item.getDescription() != null)
                sb.append("\n- Description: ").append(item.getDescription());
            if (item.getTags() != null && item.getTags().length > 0)
                sb.append("\n- Tags: ").append(Arrays.toString(item.getTags()));
            if (item.getAudience() != null && !item.getAudience().isEmpty())
                sb.append("\n- Audience: ").append(item.getAudience());
            if (item.getMetrics() != null && !item.getMetrics().isEmpty())
                sb.append("\n- Coverage: ").append(item.getMetrics());

            sb.append("\n");
        }

        sb.append("\nEvaluate each outlet against the campaign brief and return enriched recommendations.");
        return sb.toString();
    }

    // ── Response building ───────────────────────────────────────────────────────

    private RecommendationResponseDTO buildResponse(
            JsonNode json, List<MediaItem> rawCandidates, String sessionId) {

        Map<String, MediaItem> itemById = new HashMap<>();
        rawCandidates.forEach(item -> itemById.put(item.getId().toString(), item));

        List<RecommendationResponseDTO.MediaItemDTO> enriched = new ArrayList<>();

        json.path("recommendations").forEach(recNode -> {
            String idStr = recNode.path("media_item_id").asText(null);
            if (idStr == null) return;

            MediaItem item = itemById.get(idStr);
            if (item == null) {
                log.warn("[Enrichment] unknown media_item_id={}", idStr);
                return;
            }

            enriched.add(RecommendationResponseDTO.MediaItemDTO.builder()
                .id(item.getId())
                .title(item.getTitle())
                .url(item.getUrl())
                .description(item.getDescription())
                .category(item.getCategory())
                .tags(item.getTags())
                .audience(item.getAudience())
                .metrics(item.getMetrics())
                .restrictions(item.getRestrictions())
                .costUsd(item.getCostUsd())
                .similarwebVisits(item.getSimilarwebVisits())
                .ahrefsDr(item.getAhrefsDr())
                .mozDa(item.getMozDa())
                .formatType(item.getFormatType())
                .language(item.getLanguage())
                .leadTimeHours(item.getLeadTimeHours())
                .hyperlinksType(item.getHyperlinksType())
                .matchScore(recNode.path("match_score").asInt(0))
                .matchReason(recNode.path("match_reason").asText(null))
                .suggestedFormat(recNode.path("suggested_format").asText(null))
                .estimatedReach(recNode.path("estimated_reach").asText(null))
                .budgetFit(recNode.path("budget_fit").asText(null))
                .build());
        });

        enriched.sort(Comparator.comparingInt(
            (RecommendationResponseDTO.MediaItemDTO dto) -> dto.getMatchScore() != null ? dto.getMatchScore() : 0
        ).reversed());

        return RecommendationResponseDTO.builder()
            .sessionId(sessionId)
            .reasoning(json.path("reasoning").asText(null))
            .recommendations(enriched)
            .build();
    }

    private RecommendationResponseDTO buildFallbackResponse(
            List<MediaItem> rawCandidates, String sessionId) {
        log.warn("[Enrichment] fallback (no LLM) for session={}", sessionId);

        List<RecommendationResponseDTO.MediaItemDTO> items = rawCandidates.stream()
            .map(item -> RecommendationResponseDTO.MediaItemDTO.builder()
                .id(item.getId())
                .title(item.getTitle())
                .url(item.getUrl())
                .description(item.getDescription())
                .category(item.getCategory())
                .tags(item.getTags())
                .audience(item.getAudience())
                .metrics(item.getMetrics())
                .restrictions(item.getRestrictions())
                .costUsd(item.getCostUsd())
                .similarwebVisits(item.getSimilarwebVisits())
                .ahrefsDr(item.getAhrefsDr())
                .formatType(item.getFormatType())
                .language(item.getLanguage())
                .matchScore(50)
                .matchReason("Selected based on category and keyword match.")
                .build())
            .toList();

        return RecommendationResponseDTO.builder()
            .sessionId(sessionId)
            .recommendations(items)
            .build();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private record EventDateContext(LocalDate date, long daysUntil) {}

    private static EventDateContext parseEventDate(String eventDateStr) {
        if (eventDateStr == null || eventDateStr.isBlank()) return null;
        try {
            LocalDate eventDate = LocalDate.parse(eventDateStr);
            long days = ChronoUnit.DAYS.between(LocalDate.now(), eventDate);
            return new EventDateContext(eventDate, Math.max(0, days));
        } catch (Exception e) {
            log.warn("[Enrichment] could not parse event_date='{}': {}", eventDateStr, e.getMessage());
            return null;
        }
    }

    private static String formatVisits(long visits) {
        if (visits >= 1_000_000) return String.format("%.1fM", visits / 1_000_000.0);
        if (visits >= 1_000)     return String.format("%.0fK", visits / 1_000.0);
        return String.valueOf(visits);
    }
}
