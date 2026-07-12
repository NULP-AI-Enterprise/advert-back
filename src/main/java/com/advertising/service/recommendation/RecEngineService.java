package com.advertising.service.recommendation;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.repository.MediaRepository;
import com.advertising.service.debug.DebugEvents;
import com.advertising.service.openai.OpenAIService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecEngineService {

    private final MediaRepository mediaRepository;
    private final OpenAIService openAIService;

    private static final double VECTOR_THRESHOLD = 0.60;

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request,
                                                 Consumer<WebSocketMessage> debug) {
        String queryText = buildQueryText(request);
        DebugEvents.emit(debug, request.getSessionId(), "search", "embed_query",
            "Embedding Query Text",
            Map.of("query", queryText,
                   "categories", request.getCategories() != null ? request.getCategories() : List.of(),
                   "keywords",   request.getKeywords()   != null ? request.getKeywords()   : List.of(),
                   "region",     request.getRegion()     != null ? request.getRegion()     : "",
                   "country",    request.getCountry()    != null ? request.getCountry()    : "",
                   "format",     request.getFormatPreference() != null ? request.getFormatPreference() : "",
                   "max_results", request.getMaxResults()));

        return openAIService.createEmbedding(queryText)
            .map(RecEngineService::vectorToJson)
            .onErrorResume(e -> {
                log.warn("[RecEngine] embedding failed, proceeding without vector search: {}", e.getMessage());
                DebugEvents.emit(debug, request.getSessionId(), "search", "embed_error",
                    "Embedding Failed — skipping vector search", Map.of("error", e.getMessage()));
                return Mono.just("");
            })
            .flatMap(embeddingJson -> Mono.fromCallable(() -> sqlQuery(request, embeddingJson, debug))
                .subscribeOn(Schedulers.boundedElastic()))
            .doOnNext(items -> log.info("[RecEngine] {} candidates for session={}", items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] query failed: {}", e.getMessage(), e));
    }

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request) {
        return findCandidates(request, DebugEvents.NOOP);
    }

    private String buildQueryText(RecommendationRequestDTO request) {
        List<String> parts = new ArrayList<>();
        if (request.getCategories() != null) parts.addAll(request.getCategories());
        if (request.getKeywords() != null) parts.addAll(request.getKeywords());
        if (request.getTargetAudienceDescription() != null) parts.add(request.getTargetAudienceDescription());
        return String.join(" ", parts);
    }

    private static String vectorToJson(float[] vector) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request, String embeddingJson,
                                     Consumer<WebSocketMessage> debug) {
        int targetLimit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;
        int fetchLimit  = Math.max(targetLimit * 4, 40);
        String sid      = request.getSessionId();

        List<String> keywords = buildKeywords(request);
        log.info("[RecEngine] keywords={} region='{}' country='{}' fetchLimit={}",
            keywords, request.getRegion(), request.getCountry(), fetchLimit);

        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();
        Set<String> seenKeys = new HashSet<>();

        // ── Level 1: vector similarity search ─────────────────────────────────
        if (!embeddingJson.isBlank()) {
            String formatFilter = request.getFormatPreference() != null
                ? request.getFormatPreference() : "";
            int before = pool.size();
            resolveItems(mediaRepository.findSimilarByEmbeddingFiltered(
                    embeddingJson, VECTOR_THRESHOLD, fetchLimit, formatFilter))
                .forEach(item -> addToPool(item, pool, seenKeys));
            log.info("[RecEngine] after vector search (threshold={}, format='{}') → pool={}",
                VECTOR_THRESHOLD, formatFilter.isEmpty() ? "any" : formatFilter, pool.size());
            DebugEvents.emit(debug, sid, "search", "vector_search",
                "L1 Vector Search (threshold=" + VECTOR_THRESHOLD + ")",
                Map.of("threshold", VECTOR_THRESHOLD,
                       "format_filter", formatFilter.isEmpty() ? "any" : formatFilter,
                       "new_hits", pool.size() - before,
                       "pool_size", pool.size()));

            if (pool.size() < targetLimit / 2) {
                log.info("[RecEngine] sparse vector results, retrying at threshold=0.45");
                before = pool.size();
                resolveItems(mediaRepository.findSimilarByEmbeddingFiltered(
                        embeddingJson, 0.45, fetchLimit, formatFilter))
                    .forEach(item -> addToPool(item, pool, seenKeys));
                log.info("[RecEngine] after low-threshold retry → pool={}", pool.size());
                DebugEvents.emit(debug, sid, "search", "vector_retry",
                    "L1 Vector Retry (threshold=0.45 — sparse results)",
                    Map.of("threshold", 0.45, "new_hits", pool.size() - before, "pool_size", pool.size()));
            }
        }

        // ── Level 2: keyword × city/region ────────────────────────────────────
        int before2 = pool.size();
        searchByKeywords(keywords, request.getRegion(), fetchLimit, pool, seenKeys);
        if (pool.size() > before2) {
            DebugEvents.emit(debug, sid, "search", "keyword_region",
                "L2 Keyword × Region (" + (request.getRegion() != null ? request.getRegion() : "none") + ")",
                Map.of("keywords", keywords, "region", request.getRegion() != null ? request.getRegion() : "",
                       "new_hits", pool.size() - before2, "pool_size", pool.size()));
        }

        // ── Level 3: keyword × country ────────────────────────────────────────
        if (pool.size() < targetLimit && request.getCountry() != null && !request.getCountry().isBlank()) {
            String country = request.getCountry();
            int before3 = pool.size();
            searchByKeywords(keywords, country, fetchLimit, pool, seenKeys);
            if (!pool.isEmpty() && request.getRegion() != null && !request.getRegion().isBlank()) {
                request.setRelaxationNote(
                    "No results specific to " + request.getRegion() + " — showing " + country + " media");
            }
            DebugEvents.emit(debug, sid, "search", "keyword_country",
                "L3 Keyword × Country (" + country + ")",
                Map.of("keywords", keywords, "country", country,
                       "new_hits", pool.size() - before3, "pool_size", pool.size()));
        }

        // ── Level 4: keyword × no geo ─────────────────────────────────────────
        if (pool.size() < targetLimit) {
            int before4 = pool.size();
            searchByKeywords(keywords, "", fetchLimit, pool, seenKeys);
            if (!pool.isEmpty() && (request.getRegion() != null || request.getCountry() != null)) {
                request.setRelaxationNote("Showing top media by traffic — no region-specific results found");
            }
            DebugEvents.emit(debug, sid, "search", "keyword_global",
                "L4 Keyword (no geo filter)",
                Map.of("keywords", keywords, "new_hits", pool.size() - before4, "pool_size", pool.size()));
        }

        // ── Level 4a: category padding ─────────────────────────────────────────
        if (pool.size() < targetLimit && request.getCategories() != null) {
            int before4a = pool.size();
            for (String cat : request.getCategories()) {
                resolveItems(mediaRepository.findByCategory(cat, fetchLimit))
                    .forEach(item -> addToPool(item, pool, seenKeys));
                if (pool.size() >= targetLimit) break;
            }
            log.info("[RecEngine] after category padding → pool={}", pool.size());
            DebugEvents.emit(debug, sid, "search", "category_pad",
                "L4a Category Padding",
                Map.of("categories", request.getCategories(),
                       "new_hits", pool.size() - before4a, "pool_size", pool.size()));
        }

        // ── Level 4b: top-traffic padding ─────────────────────────────────────
        if (pool.size() < targetLimit) {
            log.info("[RecEngine] pool={} / target={} — padding with top-traffic items",
                pool.size(), targetLimit);
            int before4b = pool.size();
            resolveItems(mediaRepository.findTopN(fetchLimit * 2))
                .forEach(item -> addToPool(item, pool, seenKeys));
            DebugEvents.emit(debug, sid, "search", "top_traffic",
                "L4b Top-Traffic Padding (no keyword match)",
                Map.of("new_hits", pool.size() - before4b, "pool_size", pool.size()));
        }

        log.info("[RecEngine] {} total candidates (target={})", pool.size(), targetLimit);
        DebugEvents.emit(debug, sid, "search", "final_pool",
            "Final Candidate Pool → Enrichment",
            Map.of("total_candidates", pool.size(), "target_results", targetLimit,
                   "titles", pool.values().stream().limit(10)
                       .map(MediaItem::getTitle).toList()));
        return new ArrayList<>(pool.values());
    }

    private void searchByKeywords(
            List<String> keywords, String geo, int limit,
            LinkedHashMap<UUID, MediaItem> pool, Set<String> seenKeys) {

        String region = geo != null ? geo : "";
        for (String kw : keywords) {
            if (kw.length() < 4) continue;
            List<Object[]> rows = mediaRepository.findByTextAndRegion(kw, region, limit);
            resolveItems(rows).forEach(item -> addToPool(item, pool, seenKeys));
            if (pool.size() >= limit) break;
        }
    }

    private static void addToPool(MediaItem item, LinkedHashMap<UUID, MediaItem> pool, Set<String> seenKeys) {
        String key = item.getUrl() != null
            ? item.getUrl() + "|" + Objects.toString(item.getFormatType(), "")
            : item.getId().toString();
        if (seenKeys.add(key)) {
            pool.put(item.getId(), item);
        }
    }

    /**
     * Builds search terms from all available request fields.
     * Categories come pre-classified by the LLM (canonical names).
     * Keywords are free-form topic terms extracted by the LLM (crypto, blockchain, etc.).
     * Audience words add topical signal without rigid mapping.
     */
    private List<String> buildKeywords(RecommendationRequestDTO request) {
        List<String> keywords = new ArrayList<>();

        // 1. Canonical categories from LLM (Technology, Business, etc.)
        if (request.getCategories() != null) {
            keywords.addAll(request.getCategories());
        }

        // 2. Topic keywords extracted by LLM (crypto, blockchain, wallet, etc.)
        if (request.getKeywords() != null) {
            keywords.addAll(request.getKeywords());
        }

        // 3. Meaningful words from audience description
        if (request.getTargetAudienceDescription() != null) {
            Arrays.stream(request.getTargetAudienceDescription().split("[\\s,;]+"))
                .map(String::toLowerCase)
                .filter(w -> w.length() > 3)
                .filter(w -> !STOPWORDS.contains(w))
                .distinct()
                .limit(3)
                .forEach(keywords::add);
        }

        // 4. Campaign objective as last-resort signal
        if (request.getCampaignObjective() != null && !request.getCampaignObjective().isBlank()) {
            keywords.add(request.getCampaignObjective());
        }

        if (keywords.isEmpty()) keywords.add("");
        return keywords;
    }

    private static final Set<String> STOPWORDS = Set.of(
        "the", "and", "for", "with", "that", "this", "from", "are", "who",
        "ages", "age", "primarily", "their", "have", "been", "they", "will",
        "target", "audience", "demographic", "people", "users", "customers",
        "men", "women", "aged", "savvy", "based", "interested", "about",
        "looking", "focused", "those", "want", "need", "more", "also"
    );

    private List<MediaItem> resolveItems(List<Object[]> rows) {
        if (rows.isEmpty()) return List.of();
        // Collect all IDs first, then load in a single IN query (avoids N+1)
        List<UUID> ids = rows.stream()
            .map(row -> UUID.fromString((String) row[0]))
            .toList();
        Map<UUID, MediaItem> byId = mediaRepository.findAllById(ids).stream()
            .collect(Collectors.toMap(MediaItem::getId, Function.identity()));
        // Preserve the original order (sorted by traffic from SQL)
        return ids.stream()
            .map(byId::get)
            .filter(Objects::nonNull)
            .toList();
    }
}
