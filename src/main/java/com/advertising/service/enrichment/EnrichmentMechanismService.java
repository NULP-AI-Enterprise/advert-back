package com.advertising.service.enrichment;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.dto.RecommendationResponseDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.service.openai.OpenAIService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.*;

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

    private static final String SYSTEM_PROMPT = """
        You are a senior media planning strategist for the Ukrainian market.
        Your job is to evaluate PR placement opportunities against a campaign brief
        and return ONLY the best matches — not every candidate.

        You have access to REAL data for each outlet:
        - cost_usd: actual placement price from the PR marketplace
        - similarweb_visits: verified monthly traffic
        - ahrefs_dr / moz_da: SEO authority scores
        - format_type: placement format (Article, Press Release, Paid news, etc.)
        - language: publication language
        - restrictions: content categories allowed or restricted

        Scoring rules:
        - 80-100: Excellent fit — cost, audience, category, and restrictions all align
        - 60-79:  Good fit — most factors align, minor gaps
        - 40-59:  Partial fit — useful but not ideal
        - < 40:   Poor fit — EXCLUDE entirely (do not include in response)

        Budget guidance (use exact cost_usd, not vague tiers):
        - If cost_usd > budget * 0.5: flag as expensive, reduce score by 20
        - If cost_usd > budget: EXCLUDE unless the outlet is exceptionally strong
        - For multi-placement campaigns: consider how many placements fit in budget

        Restriction check — read carefully:
        - ONLY EXCLUDE if the specific content type is EXPLICITLY set to false
          (e.g., restrictions contains crypto: false → crypto content is FORBIDDEN).
        - If the key is ABSENT from restrictions or restrictions is empty → content is PERMITTED.
          Absence of a key means "not restricted", NOT "not allowed". Do NOT exclude.
        - If the key is true → content is explicitly ALLOWED, boost score by +5.

        Pre-enrichment handling:
        - Some outlets may have no description, tags, or audience data (still being processed).
        - In that case evaluate using ONLY: cost_usd, similarweb_visits, ahrefs_dr,
          format_type, language, restrictions. This is sufficient data to score.
        - A missing description does NOT mean poor fit. Default base score: 55.
          Adjust up/down based on cost fit, traffic volume, and format match.
        - NEVER score below 40 solely because description is missing.

        For each included item provide:
        - match_score: 40-100
        - match_reason: 2-3 sentences citing SPECIFIC numbers. Example:
          "1.6M monthly visits (SimilarWeb) with DR=77 — strong SEO authority.
          Cost $130 fits well within $2000 budget (15% of monthly spend).
          Ukrainian + Russian language aligns with target audience."
        - suggested_format: best format for this campaign from available format_type
          (for leads → Article or Paid news; for awareness → Press Release)
        - estimated_reach: concrete estimate, e.g. "~1.6M monthly readers, national reach"
        - budget_fit: how cost fits, e.g. "costs $130 of $2000 budget — fits 15× per month"

        Also return a top-level `reasoning` field (3-5 sentences) explaining:
        - What the overall budget allows
        - Why the selected outlets match the target audience
        - Any notable trade-offs or gaps

        Return ONLY valid JSON — no markdown, no prose:
        {
          "reasoning": "string",
          "recommendations": [
            {
              "media_item_id": "uuid-string",
              "match_score": 40-100,
              "match_reason": "string",
              "suggested_format": "string",
              "estimated_reach": "string",
              "budget_fit": "string"
            }
          ]
        }

        Sort recommendations by match_score descending. Return at most 10 items.
        """;

    public Mono<RecommendationResponseDTO> enrich(
            List<MediaItem> rawCandidates,
            RecommendationRequestDTO request) {

        if (rawCandidates.isEmpty()) {
            log.warn("[Enrichment] no candidates for session={}", request.getSessionId());
            return Mono.just(RecommendationResponseDTO.builder()
                .sessionId(request.getSessionId())
                .recommendations(List.of())
                .build());
        }

        log.info("[Enrichment] enriching {} candidates for session={}",
            rawCandidates.size(), request.getSessionId());

        List<Map<String, String>> messages = buildMessages(rawCandidates, request);

        return openAIService.chatCompletionJson(messages)
            .map(json -> buildResponse(json, rawCandidates, request.getSessionId()))
            .doOnNext(resp -> log.info("[Enrichment] {} recommendations for session={}",
                resp.getRecommendations().size(), request.getSessionId()))
            .doOnError(e -> log.error("[Enrichment] LLM failed: {}", e.getMessage(), e))
            .onErrorReturn(buildFallbackResponse(rawCandidates, request.getSessionId()));
    }

    private List<Map<String, String>> buildMessages(
            List<MediaItem> candidates, RecommendationRequestDTO request) {
        return List.of(
            Map.of("role", "system", "content", SYSTEM_PROMPT),
            Map.of("role", "user",   "content", buildUserPrompt(candidates, request))
        );
    }

    private String buildUserPrompt(List<MediaItem> candidates, RecommendationRequestDTO request) {
        StringBuilder sb = new StringBuilder();

        sb.append("## Campaign Brief\n");
        if (request.getCategories() != null && !request.getCategories().isEmpty()) {
            sb.append("- Categories: ").append(String.join(", ", request.getCategories())).append("\n");
        }
        if (request.getTargetAudienceDescription() != null) {
            sb.append("- Target Audience: ").append(request.getTargetAudienceDescription()).append("\n");
        }
        if (request.getCampaignObjective() != null) {
            sb.append("- Objective: ").append(request.getCampaignObjective()).append("\n");
        }
        if (request.getBudgetUsd() != null) {
            sb.append("- Budget: $").append(String.format("%.0f", request.getBudgetUsd())).append(" USD\n");
        }
        if (request.getAgeRange() != null) {
            sb.append("- Age Range: ")
                .append(request.getAgeRange().getMin()).append("-")
                .append(request.getAgeRange().getMax()).append("\n");
        }
        if (request.getRegion() != null) {
            sb.append("- Region: ").append(request.getRegion()).append("\n");
        }
        if (request.getRelaxationNote() != null) {
            sb.append("- Note: ").append(request.getRelaxationNote()).append("\n");
        }

        sb.append("\n## Candidate Outlets\n");
        for (MediaItem item : candidates) {
            sb.append("\n### ").append(item.getTitle());
            sb.append("\n- ID: ").append(item.getId());

            if (item.getUrl() != null) {
                sb.append("\n- URL: ").append(item.getUrl());
            }

            // Placement specs
            if (item.getCostUsd() != null) {
                sb.append("\n- Cost: $").append(item.getCostUsd()).append(" USD");
            }
            if (item.getFormatType() != null) {
                sb.append("\n- Format: ").append(item.getFormatType());
            }
            if (item.getLanguage() != null) {
                sb.append("\n- Language: ").append(item.getLanguage());
            }
            if (item.getLeadTimeHours() != null) {
                sb.append("\n- Lead Time: ").append(item.getLeadTimeHours()).append("h");
            }
            if (item.getHyperlinksType() != null) {
                sb.append("\n- Hyperlinks: ").append(item.getHyperlinksType());
            }

            // Traffic & SEO
            if (item.getSimilarwebVisits() != null) {
                sb.append("\n- Traffic: ").append(formatVisits(item.getSimilarwebVisits()))
                  .append(" monthly visits (SimilarWeb)");
            }
            if (item.getAhrefsDr() != null || item.getMozDa() != null || item.getSemrushScore() != null) {
                sb.append("\n- SEO:");
                if (item.getAhrefsDr() != null)    sb.append(" DR=").append(item.getAhrefsDr());
                if (item.getMozDa() != null)        sb.append(" DA=").append(item.getMozDa());
                if (item.getSemrushScore() != null) sb.append(" Semrush=").append(item.getSemrushScore());
            }

            // Restrictions
            if (item.getRestrictions() != null && !item.getRestrictions().isEmpty()) {
                List<String> allowed    = new ArrayList<>();
                List<String> restricted = new ArrayList<>();
                item.getRestrictions().forEach((k, v) -> {
                    if (Boolean.TRUE.equals(v))  allowed.add(k);
                    if (Boolean.FALSE.equals(v)) restricted.add(k);
                });
                if (!allowed.isEmpty())    sb.append("\n- Allowed: ").append(String.join(", ", allowed));
                if (!restricted.isEmpty()) sb.append("\n- Restricted: ").append(String.join(", ", restricted));
            }

            // LLM-enriched fields (may be null before enricher runs)
            if (item.getCategory() != null) {
                sb.append("\n- Category: ").append(item.getCategory());
            }
            if (item.getDescription() != null) {
                sb.append("\n- Description: ").append(item.getDescription());
            }
            if (item.getTags() != null && item.getTags().length > 0) {
                sb.append("\n- Tags: ").append(Arrays.toString(item.getTags()));
            }
            if (item.getAudience() != null && !item.getAudience().isEmpty()) {
                sb.append("\n- Audience: ").append(item.getAudience());
            }
            if (item.getMetrics() != null && !item.getMetrics().isEmpty()) {
                sb.append("\n- Coverage: ").append(item.getMetrics());
            }
            sb.append("\n");
        }

        sb.append("\nEvaluate each outlet against the campaign brief and return enriched recommendations.");
        return sb.toString();
    }

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
                // Structured placement data
                .costUsd(item.getCostUsd())
                .similarwebVisits(item.getSimilarwebVisits())
                .ahrefsDr(item.getAhrefsDr())
                .mozDa(item.getMozDa())
                .formatType(item.getFormatType())
                .language(item.getLanguage())
                .leadTimeHours(item.getLeadTimeHours())
                .hyperlinksType(item.getHyperlinksType())
                // LLM reasoning
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

    private static String formatVisits(long visits) {
        if (visits >= 1_000_000) return String.format("%.1fM", visits / 1_000_000.0);
        if (visits >= 1_000)     return String.format("%.0fK", visits / 1_000.0);
        return String.valueOf(visits);
    }
}
