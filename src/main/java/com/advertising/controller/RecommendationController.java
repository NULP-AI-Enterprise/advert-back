package com.advertising.controller;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.dto.RecommendationResponseDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import com.advertising.service.enrichment.EnrichmentMechanismService;
import com.advertising.service.enrichment.EnrichmentService;
import com.advertising.service.recommendation.RecEngineService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/recommendations")
@RequiredArgsConstructor
public class RecommendationController {

    private final RecEngineService recEngineService;
    private final EnrichmentMechanismService enrichmentMechanismService;
    private final MediaRepository mediaRepository;
    private final EnrichmentService enrichmentService;

    /**
     * REST endpoint for direct recommendation queries (bypasses WebSocket).
     * V1: SQL candidate retrieval → LLM enrichment.
     */
    @PostMapping("/search")
    public Mono<ResponseEntity<RecommendationResponseDTO>> search(
            @RequestBody RecommendationRequestDTO request) {

        return recEngineService.findCandidates(request)
            .flatMap(candidates -> enrichmentMechanismService.enrich(candidates, request))
            .map(ResponseEntity::ok);
    }

    /**
     * Ingests a new media item and triggers async embedding enrichment.
     * Kept for backward compatibility — embeddings are still stored but not used for V1 rec engine.
     */
    @PostMapping("/media")
    public ResponseEntity<MediaItem> createMediaItem(@RequestBody MediaItem item) {
        MediaItem saved = mediaRepository.save(item);
        enrichmentService.enrichMediaItem(saved.getId());
        return ResponseEntity.status(HttpStatus.CREATED).body(saved);
    }

    /**
     * Manually triggers re-enrichment (embedding refresh) for a single item.
     */
    @PostMapping("/media/{id}/enrich")
    public ResponseEntity<Void> triggerEnrichment(@PathVariable UUID id) {
        enrichmentService.enrichMediaItem(id);
        return ResponseEntity.accepted().build();
    }
}
