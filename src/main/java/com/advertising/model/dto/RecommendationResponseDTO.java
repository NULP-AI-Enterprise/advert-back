package com.advertising.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

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
    private String explanation;
    private Map<String, Object> appliedFilters;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MediaItemDTO {
        private UUID id;
        private String title;
        private String description;
        private String category;
        private String[] tags;
        private Map<String, Object> audience;
        private Map<String, Object> metrics;
        private Double similarityScore;

        // V1 enrichment fields — populated by EnrichmentMechanismService
        private Integer matchScore;
        private String matchReason;
        private String suggestedFormat;
        private String estimatedReach;
    }
}
