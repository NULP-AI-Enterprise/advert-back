package com.advertising.controller;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.dto.RecommendationResponseDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import com.advertising.service.enrichment.EnrichmentMechanismService;
import com.advertising.service.enrichment.EnrichmentService;
import com.advertising.service.recommendation.RecEngineService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;

@WebMvcTest(RecommendationController.class)
@Import(TestSecurityConfig.class)
@ActiveProfiles("test")
@DisplayName("RecommendationController")
class RecommendationControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockBean RecEngineService            recEngineService;
    @MockBean EnrichmentMechanismService  enrichmentMechanismService;
    @MockBean EnrichmentService           enrichmentService;
    @MockBean MediaRepository             mediaRepository;

    private RecommendationResponseDTO sampleResponse;
    private MediaItem sampleMedia;

    @BeforeEach
    void setUp() {
        RecommendationResponseDTO.MediaItemDTO item = RecommendationResponseDTO.MediaItemDTO.builder()
                .id(UUID.randomUUID())
                .title("Prime-time TV Spot")
                .category("Television")
                .matchScore(85)
                .matchReason("Strong alignment with FMCG mass-market audience")
                .suggestedFormat("30s commercial")
                .estimatedReach("12M viewers")
                .build();

        sampleResponse = RecommendationResponseDTO.builder()
                .recommendations(List.of(item))
                .reasoning("Best match based on your FMCG brief")
                .build();

        sampleMedia = MediaItem.builder()
                .id(UUID.randomUUID())
                .title("Morning News Segment")
                .category("Television")
                .description("Live morning news with 3M daily viewers")
                .build();
    }

    // ──────────────────────────────────────────────────────
    // POST /recommendations/search
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("POST /recommendations/search")
    class Search {

        @Test
        @DisplayName("returns 200 with enriched recommendations")
        void validRequest_returns200WithResults() throws Exception {
            when(recEngineService.findCandidates(any())).thenReturn(Mono.just(List.of(sampleMedia)));
            when(enrichmentMechanismService.enrich(any(), any())).thenReturn(Mono.just(sampleResponse));

            RecommendationRequestDTO request = RecommendationRequestDTO.builder()
                    .campaignObjective("awareness")
                    .categories(List.of("Television"))
                    .maxResults(5)
                    .build();

            var mvcResult = mockMvc.perform(post("/recommendations/search")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(mvcResult))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.recommendations", hasSize(1)))
                    .andExpect(jsonPath("$.recommendations[0].title", is("Prime-time TV Spot")))
                    .andExpect(jsonPath("$.recommendations[0].matchScore", is(85)))
                    .andExpect(jsonPath("$.reasoning", notNullValue()));
        }

        @Test
        @DisplayName("returns 200 with empty list when no candidates match")
        void noCandidates_returnsEmptyList() throws Exception {
            RecommendationResponseDTO emptyResponse = RecommendationResponseDTO.builder()
                    .recommendations(List.of())
                    .build();

            when(recEngineService.findCandidates(any())).thenReturn(Mono.just(List.of()));
            when(enrichmentMechanismService.enrich(any(), any())).thenReturn(Mono.just(emptyResponse));

            var mvcResult = mockMvc.perform(post("/recommendations/search")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(RecommendationRequestDTO.builder().build())))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(mvcResult))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.recommendations", hasSize(0)));
        }

        @Test
        @DisplayName("returns 400 when request body is missing")
        void missingBody_returns400() throws Exception {
            mockMvc.perform(post("/recommendations/search")
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isBadRequest());
        }
    }

    // ──────────────────────────────────────────────────────
    // POST /recommendations/media
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("POST /recommendations/media")
    class CreateMedia {

        @Test
        @DisplayName("saves media item and triggers async enrichment, returns 201")
        void validItem_returns201() throws Exception {
            when(mediaRepository.save(any(MediaItem.class))).thenReturn(sampleMedia);
            doNothing().when(enrichmentService).enrichMediaItem(any(UUID.class));

            mockMvc.perform(post("/recommendations/media")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(sampleMedia)))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.title", is("Morning News Segment")))
                    .andExpect(jsonPath("$.category", is("Television")));

            verify(mediaRepository).save(any(MediaItem.class));
            verify(enrichmentService).enrichMediaItem(sampleMedia.getId());
        }

        @Test
        @DisplayName("returns 400 when request body is missing")
        void missingBody_returns400() throws Exception {
            mockMvc.perform(post("/recommendations/media")
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isBadRequest());
        }
    }

    // ──────────────────────────────────────────────────────
    // POST /recommendations/media/{id}/enrich
    // ──────────────────────────────────────────────────────
    @Nested @DisplayName("POST /recommendations/media/{id}/enrich")
    class TriggerEnrichment {

        @Test
        @DisplayName("returns 202 Accepted and triggers enrichment")
        void existingId_returns202() throws Exception {
            UUID itemId = UUID.randomUUID();
            doNothing().when(enrichmentService).enrichMediaItem(itemId);

            mockMvc.perform(post("/recommendations/media/{id}/enrich", itemId))
                    .andExpect(status().isAccepted());

            verify(enrichmentService).enrichMediaItem(itemId);
        }

        @Test
        @DisplayName("returns 400 when id is not a valid UUID")
        void invalidUuid_returns400() throws Exception {
            mockMvc.perform(post("/recommendations/media/{id}/enrich", "not-a-uuid"))
                    .andExpect(status().isBadRequest());
        }
    }
}
