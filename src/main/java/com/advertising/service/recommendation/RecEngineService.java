package com.advertising.service.recommendation;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * V1 Recommendation Engine — pure SQL-based candidate retrieval.
 * No vector search, no embeddings.
 *
 * Flow:
 *  1. If categories provided → filter by category (findByTextFallback with category keyword)
 *  2. Fallback → return top-N items by recency
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RecEngineService {

    private final MediaRepository mediaRepository;

    /**
     * Finds raw candidate MediaItems from DB using structured SQL params.
     * Blocking JPA calls are wrapped in Mono.fromCallable + boundedElastic scheduler.
     *
     * @param request structured params from AgenticLoopService
     * @return Mono with list of raw MediaItem candidates
     */
    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request) {
        return Mono.fromCallable(() -> sqlQuery(request))
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(items -> log.info("[RecEngine] findCandidates → {} items for session={}",
                items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] SQL query failed: {}", e.getMessage(), e));
    }

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request) {
        int limit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;

        // Try category-based SQL search first
        if (request.getCategories() != null && !request.getCategories().isEmpty()) {
            for (String category : request.getCategories()) {
                String keyword = mapCategoryKeyword(category);
                List<Object[]> rows = mediaRepository.findByTextFallback(keyword, limit);
                if (!rows.isEmpty()) {
                    log.info("[RecEngine] category='{}' → keyword='{}' → {} results",
                        category, keyword, rows.size());
                    return resolveItems(rows);
                }
            }
        }

        // Fallback: try audience description keyword
        if (request.getTargetAudienceDescription() != null
                && !request.getTargetAudienceDescription().isBlank()) {
            String keyword = firstWord(request.getTargetAudienceDescription());
            List<Object[]> rows = mediaRepository.findByTextFallback(keyword, limit);
            if (!rows.isEmpty()) {
                log.info("[RecEngine] audience keyword='{}' → {} results", keyword, rows.size());
                return resolveItems(rows);
            }
        }

        // Last resort: top-N newest items
        log.warn("[RecEngine] no category/audience match — returning top-N");
        return resolveItems(mediaRepository.findTopN(limit));
    }

    /**
     * Maps English category names to Ukrainian/local DB values for better matching.
     */
    private String mapCategoryKeyword(String category) {
        return switch (category.toLowerCase()) {
            case "technology", "software", "it", "tech" -> "Технології";
            case "business", "b2b", "finance"           -> "Бізнес";
            case "news"                                  -> "Новини";
            case "sport", "sports"                      -> "Спорт";
            case "fashion", "lifestyle"                  -> "Мода";
            case "video", "tv"                           -> "Відео";
            case "agriculture", "agro"                  -> "Агро";
            default -> category;
        };
    }

    private String firstWord(String text) {
        if (text == null || text.isBlank()) return "бізнес";
        String trimmed = text.trim();
        return trimmed.contains(" ") ? trimmed.substring(0, trimmed.indexOf(' ')) : trimmed;
    }

    /**
     * Resolves raw Object[] rows (id as text, score) into full MediaItem entities.
     * row[0] = CAST(id AS text), row[1] = similarity score (unused in V1, kept for compat)
     */
    private List<MediaItem> resolveItems(List<Object[]> rows) {
        return rows.stream()
            .map(row -> {
                UUID id = UUID.fromString((String) row[0]);
                return mediaRepository.findById(id).orElse(null);
            })
            .filter(Objects::nonNull)
            .toList();
    }
}
