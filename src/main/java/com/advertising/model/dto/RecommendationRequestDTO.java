package com.advertising.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

/**
 * Structured params extracted by the Agentic Loop (LLM) for a media recommendation search.
 * Used by RecEngineService (SQL query) and EnrichmentMechanismService (LLM enrichment context).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RecommendationRequestDTO {

    private String sessionId;

    // SQL filter params extracted from campaign brief
    private List<String> categories;
    private List<String> keywords;              // free-form topic terms (crypto, blockchain, etc.)
    private String targetAudienceDescription;
    private AgeRange ageRange;
    private Double budgetUsd;
    private String campaignObjective;           // awareness | leads | conversions | engagement
    private String region;                      // city / district level
    private String country;                     // country level (fallback for region)
    private String minReachTier;                // national | regional | local | niche
    private String formatPreference;            // Article | Press Release | Paid news | Video | any
    private String eventDate;                   // ISO date string when campaign is for a timed event
    private String relaxationNote;              // set by RecEngineService when geo is broadened

    private int maxResults;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgeRange {
        private Integer min;
        private Integer max;
    }
}
