package com.advertising.service.enrichment;

import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import com.advertising.service.openai.OpenAIService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Background enrichment tasks — run asynchronously so they don't block the chat loop.
 * Computes/refreshes embeddings and LLM-extracted metadata for media items.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EnrichmentService {

    private final MediaRepository mediaRepository;
    private final OpenAIService openAIService;

    /**
     * (Re)generates the embedding for a single media item.
     * Called after creation or manual re-enrichment requests.
     */
    @Async("enrichmentExecutor")
    public void enrichMediaItem(UUID mediaItemId) {
        mediaRepository.findById(mediaItemId).ifPresentOrElse(item -> {
            String textToEmbed = buildEnrichmentText(item);

            openAIService.createEmbedding(textToEmbed)
                .doOnNext(embedding -> {
                    // Use native query to avoid Hibernate bytea/vector type mismatch
                    String vecStr = Arrays.toString(embedding).replace(" ", "");
                    mediaRepository.updateEmbeddingById(mediaItemId.toString(), vecStr);
                    log.info("Enriched media item {} with embedding ({}d)", mediaItemId, embedding.length);
                })
                .doOnError(e -> log.error("Failed to enrich media item {}", mediaItemId, e))
                .subscribe();
        }, () -> log.warn("Media item {} not found for enrichment", mediaItemId));
    }

    /**
     * Asks the LLM to analyse a new media item and extract structured metadata.
     */
    @Async("enrichmentExecutor")
    public void analyzeMediaItem(UUID mediaItemId) {
        mediaRepository.findById(mediaItemId).ifPresent(item -> {
            List<Map<String, String>> messages = List.of(
                Map.of("role", "system", "content", """
                    You are a media analyst. Given a media item's title and description,
                    extract structured metadata as JSON:
                    {
                      "audience": { "age_range": "", "interests": [], "demographics": {} },
                      "category_tags": [],
                      "campaign_fit": ["awareness", "conversion", "engagement"]
                    }
                    Respond ONLY with valid JSON.
                    """),
                Map.of("role", "user", "content",
                    "Title: " + item.getTitle() + "\nDescription: " + item.getDescription())
            );

            openAIService.chatCompletionJson(messages)
                .doOnNext(json -> {
                    log.info("Analysis complete for media item {}: {}", mediaItemId, json);
                    // TODO: map json back to item.audience / item.tags and save
                })
                .doOnError(e -> log.error("LLM analysis failed for media item {}", mediaItemId, e))
                .subscribe();
        });
    }

    private String buildEnrichmentText(MediaItem item) {
        StringBuilder sb = new StringBuilder();
        sb.append(item.getTitle()).append(". ");
        if (item.getDescription() != null) sb.append(item.getDescription()).append(" ");
        if (item.getCategory() != null) sb.append("Category: ").append(item.getCategory()).append(". ");
        if (item.getTags() != null) sb.append("Tags: ").append(String.join(", ", item.getTags()));
        return sb.toString();
    }
}
