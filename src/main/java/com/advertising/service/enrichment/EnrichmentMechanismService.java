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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * V1 Enrichment Mechanism — takes raw SQL candidates and uses GPT-4o to produce
 * enriched recommendations with match_reason, suggested_format, estimated_reach
 * and match_score for each media item.
 *
 * Flow:
 *  rawCandidates (List<MediaItem>) + campaign context (RecommendationRequestDTO)
 *      → LLM prompt → JSON response
 *      → Mono<RecommendationResponseDTO> sorted by matchScore desc
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EnrichmentMechanismService {

    private final OpenAIService openAIService;

    private static final String ENRICHMENT_SYSTEM_PROMPT = """
        You are a media placement strategist. Given a campaign brief and a list of media items,
        evaluate each placement and return structured enrichment data.

        For each media item, provide:
        - match_score: integer 0-100 (how well this placement fits the campaign)
        - match_reason: concise explanation of why this placement fits the campaign goals
        - suggested_format: recommended ad format (e.g. Banner, Pre-roll, Native, Sponsored Post)
        - estimated_reach: estimated audience reach description (e.g. "150K monthly users, 25-40 age group")

        Return ONLY valid JSON with this schema:
        {
          "recommendations": [
            {
              "media_item_id": "uuid-string",
              "match_score": 0-100,
              "match_reason": "string",
              "suggested_format": "string",
              "estimated_reach": "string"
            }
          ]
        }

        Sort results by match_score descending. Only include items you can meaningfully evaluate.
        """;

    /**
     * Enriches raw media item candidates with LLM-generated match context.
     *
     * @param rawCandidates list of raw MediaItem results from RecEngineService
     * @param request       campaign context from the agentic loop
     * @return Mono with fully enriched RecommendationResponseDTO sorted by matchScore
     */
    public Mono<RecommendationResponseDTO> enrich(
            List<MediaItem> rawCandidates,
            RecommendationRequestDTO request) {

        if (rawCandidates.isEmpty()) {
            log.warn("[Enrichment] no candidates to enrich for session={}", request.getSessionId());
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
            .doOnNext(resp -> log.info("[Enrichment] enriched {} items for session={}",
                resp.getRecommendations().size(), request.getSessionId()))
            .doOnError(e -> log.error("[Enrichment] LLM call failed: {}", e.getMessage(), e))
            .onErrorReturn(buildFallbackResponse(rawCandidates, request.getSessionId()));
    }

    private List<Map<String, String>> buildMessages(
            List<MediaItem> candidates,
            RecommendationRequestDTO request) {

        List<Map<String, String>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", ENRICHMENT_SYSTEM_PROMPT));

        String userPrompt = buildUserPrompt(candidates, request);
        messages.add(Map.of("role", "user", "content", userPrompt));

        return messages;
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
            sb.append("- Budget: $").append(request.getBudgetUsd()).append(" USD\n");
        }
        if (request.getAgeRange() != null) {
            sb.append("- Age Range: ")
                .append(request.getAgeRange().getMin()).append("-")
                .append(request.getAgeRange().getMax()).append("\n");
        }
        if (request.getRegion() != null) {
            sb.append("- Region: ").append(request.getRegion()).append("\n");
        }

        sb.append("\n## Media Items\n");
        for (MediaItem item : candidates) {
            sb.append("\n### ID: ").append(item.getId()).append("\n");
            sb.append("- Title: ").append(item.getTitle()).append("\n");
            if (item.getDescription() != null) {
                sb.append("- Description: ").append(item.getDescription()).append("\n");
            }
            if (item.getCategory() != null) {
                sb.append("- Category: ").append(item.getCategory()).append("\n");
            }
            if (item.getTags() != null && item.getTags().length > 0) {
                sb.append("- Tags: ").append(Arrays.toString(item.getTags())).append("\n");
            }
            if (item.getAudience() != null) {
                sb.append("- Audience: ").append(item.getAudience()).append("\n");
            }
            if (item.getMetrics() != null) {
                sb.append("- Metrics: ").append(item.getMetrics()).append("\n");
            }
        }

        sb.append("\nPlease evaluate each media item against the campaign brief and return enriched recommendations.");
        return sb.toString();
    }

    private RecommendationResponseDTO buildResponse(
            JsonNode json,
            List<MediaItem> rawCandidates,
            String sessionId) {

        // Build a lookup map from UUID string → MediaItem for fast resolution
        Map<String, MediaItem> itemById = new java.util.HashMap<>();
        rawCandidates.forEach(item -> itemById.put(item.getId().toString(), item));

        List<RecommendationResponseDTO.MediaItemDTO> enriched = new ArrayList<>();

        json.path("recommendations").forEach(recNode -> {
            String idStr = recNode.path("media_item_id").asText(null);
            if (idStr == null) return;

            MediaItem item = itemById.get(idStr);
            if (item == null) {
                log.warn("[Enrichment] LLM returned unknown media_item_id={}", idStr);
                return;
            }

            enriched.add(RecommendationResponseDTO.MediaItemDTO.builder()
                .id(item.getId())
                .title(item.getTitle())
                .description(item.getDescription())
                .category(item.getCategory())
                .tags(item.getTags())
                .audience(item.getAudience())
                .metrics(item.getMetrics())
                .matchScore(recNode.path("match_score").asInt(0))
                .matchReason(recNode.path("match_reason").asText(null))
                .suggestedFormat(recNode.path("suggested_format").asText(null))
                .estimatedReach(recNode.path("estimated_reach").asText(null))
                .build());
        });

        // Sort by matchScore descending (LLM should already sort, but enforce here)
        enriched.sort(Comparator.comparingInt(
            (RecommendationResponseDTO.MediaItemDTO dto) -> dto.getMatchScore() != null ? dto.getMatchScore() : 0
        ).reversed());

        return RecommendationResponseDTO.builder()
            .sessionId(sessionId)
            .recommendations(enriched)
            .build();
    }

    /**
     * Fallback response when LLM enrichment fails — returns raw items without enrichment data.
     */
    private RecommendationResponseDTO buildFallbackResponse(List<MediaItem> rawCandidates, String sessionId) {
        log.warn("[Enrichment] using fallback response (no LLM enrichment) for session={}", sessionId);

        List<RecommendationResponseDTO.MediaItemDTO> items = rawCandidates.stream()
            .map(item -> RecommendationResponseDTO.MediaItemDTO.builder()
                .id(item.getId())
                .title(item.getTitle())
                .description(item.getDescription())
                .category(item.getCategory())
                .tags(item.getTags())
                .audience(item.getAudience())
                .metrics(item.getMetrics())
                .matchScore(50)
                .matchReason("Candidate selected based on category and keyword matching.")
                .build())
            .toList();

        return RecommendationResponseDTO.builder()
            .sessionId(sessionId)
            .recommendations(items)
            .build();
    }
}
