package com.advertising.controller;

import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import com.advertising.service.enrichment.EnrichmentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final MediaRepository mediaRepository;
    private final EnrichmentService enrichmentService;
    private final WebClient.Builder webClientBuilder;

    @Value("${enrichment.url:http://localhost:8001}")
    private String enrichmentUrl;

    @GetMapping("/media-items")
    public ResponseEntity<Map<String, Object>> listMediaItems(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "") String search,
            @RequestParam(defaultValue = "") String category
    ) {
        PageRequest pageable = PageRequest.of(page - 1, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<MediaItem> result = mediaRepository.findByFilters(search, category, pageable);

        return ResponseEntity.ok(Map.of(
                "items", result.getContent(),
                "total", result.getTotalElements(),
                "page", page,
                "pageSize", size
        ));
    }

    @PostMapping("/media-items")
    public ResponseEntity<MediaItem> createMediaItem(@RequestBody MediaItem item) {
        item.setId(null);
        MediaItem saved = mediaRepository.save(item);
        return ResponseEntity.status(201).body(saved);
    }

    @PutMapping("/media-items/{id}")
    public ResponseEntity<MediaItem> updateMediaItem(
            @PathVariable UUID id,
            @RequestBody MediaItem body
    ) {
        return mediaRepository.findById(id).map(existing -> {
            existing.setTitle(body.getTitle());
            existing.setDescription(body.getDescription());
            existing.setCategory(body.getCategory());
            existing.setTags(body.getTags());
            existing.setAudience(body.getAudience());
            existing.setMetrics(body.getMetrics());
            return ResponseEntity.ok(mediaRepository.save(existing));
        }).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/media-items/{id}")
    public ResponseEntity<Void> deleteMediaItem(@PathVariable UUID id) {
        if (!mediaRepository.existsById(id)) return ResponseEntity.notFound().build();
        mediaRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/media-items/{id}/reenrich")
    public ResponseEntity<Void> reenrichItem(@PathVariable UUID id) {
        MediaItem item = mediaRepository.findById(id).orElse(null);
        if (item == null) return ResponseEntity.notFound().build();

        webClientBuilder.build()
                .post()
                .uri(enrichmentUrl + "/enrich/item/{id}?title={title}", id, item.getTitle())
                .retrieve()
                .toBodilessEntity()
                .doOnError(e -> log.warn("Enrichment service unreachable for item {}: {}", id, e.getMessage()))
                .subscribe();

        return ResponseEntity.accepted().build();
    }

    @PostMapping("/enrich-all")
    public ResponseEntity<Void> enrichAll() {
        webClientBuilder.build()
                .post()
                .uri(enrichmentUrl + "/enrich/start")
                .retrieve()
                .toBodilessEntity()
                .doOnError(e -> log.warn("Enrichment service unreachable: {}", e.getMessage()))
                .subscribe();

        return ResponseEntity.accepted().build();
    }
}
