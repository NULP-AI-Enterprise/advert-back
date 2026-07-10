package com.advertising.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RecommendationResponseDTO {

    private String sessionId;
    private List<MediaItemDTO> recommendations;

    /** LLM chain-of-thought explaining the overall selection logic. */
    private String reasoning;

    private Map<String, Object> appliedFilters;
    private String ctaMessage;
    private List<String> suggestions;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MediaItemDTO {
        private UUID   id;
        private String title;
        private String url;
        private String description;
        private String category;
        private String[] tags;
        private Map<String, Object> audience;
        private Map<String, Object> metrics;
        private Map<String, Object> restrictions;
        private Double similarityScore;

        // Structured placement data (direct from DB, no LLM needed)
        private BigDecimal costUsd;
        private Long       similarwebVisits;
        private Integer    ahrefsDr;
        private Integer    mozDa;
        private String     formatType;
        private String     language;
        private Integer    leadTimeHours;
        private String     hyperlinksType;

        // LLM reasoning fields
        private Integer matchScore;
        private String  matchReason;
        private String  suggestedFormat;
        private String  estimatedReach;
        private String  budgetFit;
    }
}
