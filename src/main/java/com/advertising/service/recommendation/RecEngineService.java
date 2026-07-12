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
            "Embedding Query → OpenAI",
            Map.of("query_text",  queryText,
                   "region",      request.getRegion()  != null ? request.getRegion()  : "any",
                   "country",     request.getCountry() != null ? request.getCountry() : "any",
                   "format",      request.getFormatPreference() != null ? request.getFormatPreference() : "any",
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

        // ── Build keyword list with source breakdown ───────────────────────────
        KeywordsResult kwr = buildKeywordsDetailed(request);
        log.info("[RecEngine] keywords={} region='{}' country='{}' fetchLimit={}",
            kwr.all(), request.getRegion(), request.getCountry(), fetchLimit);

        Map<String, Object> kwDebug = new LinkedHashMap<>();
        kwDebug.put("final_keyword_list", kwr.all());
        kwDebug.put("from_categories",    kwr.fromCategories());
        kwDebug.put("from_llm_keywords",  kwr.fromKeywords());
        kwDebug.put("from_audience_text", kwr.fromAudience());
        kwDebug.put("from_objective",     kwr.fromObjective());
        kwDebug.put("fetch_limit",        fetchLimit);
        kwDebug.put("target_results",     targetLimit);
        DebugEvents.emit(debug, sid, "search", "keywords_built",
            "Keywords Built (" + kwr.all().size() + " terms from 4 sources)", kwDebug);

        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();
        Set<String> seenKeys = new HashSet<>();

        // ── Level 1: vector similarity search ─────────────────────────────────
        if (!embeddingJson.isBlank()) {
            String formatFilter = request.getFormatPreference() != null
                ? request.getFormatPreference() : "";
            log.info("[RecEngine][DB] findSimilarByEmbeddingFiltered threshold={} limit={} format='{}'",
                VECTOR_THRESHOLD, fetchLimit, formatFilter.isEmpty() ? "any" : formatFilter);
            List<Object[]> vecRows = mediaRepository.findSimilarByEmbeddingFiltered(
                embeddingJson, VECTOR_THRESHOLD, fetchLimit, formatFilter);
            log.info("[RecEngine][DB] findSimilarByEmbeddingFiltered returned {} rows", vecRows.size());
            // Build id→score map before resolving to keep similarity values
            Map<String, Double> scores = new LinkedHashMap<>();
            for (Object[] row : vecRows) scores.put((String) row[0], ((Number) row[1]).doubleValue());

            int before = pool.size();
            resolveItems(vecRows).forEach(item -> addToPool(item, pool, seenKeys));
            log.info("[RecEngine] after vector search (threshold={}, format='{}') → pool={}",
                VECTOR_THRESHOLD, formatFilter.isEmpty() ? "any" : formatFilter, pool.size());

            // Show top hits with similarity scores
            List<Map<String, Object>> topHits = pool.values().stream().limit(8).map(item -> {
                Map<String, Object> h = new LinkedHashMap<>();
                h.put("title",    item.getTitle());
                h.put("score",    String.format("%.3f", scores.getOrDefault(item.getId().toString(), 0.0)));
                h.put("category", item.getCategory() != null ? item.getCategory() : "—");
                h.put("format",   item.getFormatType() != null ? item.getFormatType() : "—");
                return h;
            }).toList();
            DebugEvents.emit(debug, sid, "search", "vector_search",
                "L1 Vector Search — threshold=" + VECTOR_THRESHOLD
                    + (formatFilter.isEmpty() ? "" : ", format=" + formatFilter),
                Map.of("threshold",    VECTOR_THRESHOLD,
                       "format_filter", formatFilter.isEmpty() ? "any" : formatFilter,
                       "sql_returned",  vecRows.size(),
                       "new_in_pool",   pool.size() - before,
                       "pool_size",     pool.size(),
                       "top_hits",      topHits));

            if (pool.size() < targetLimit / 2) {
                log.info("[RecEngine] sparse vector results (pool={}), retrying at threshold=0.45", pool.size());
                log.info("[RecEngine][DB] findSimilarByEmbeddingFiltered retry threshold=0.45 limit={} format='{}'",
                    fetchLimit, formatFilter.isEmpty() ? "any" : formatFilter);
                List<Object[]> retryRows = mediaRepository.findSimilarByEmbeddingFiltered(
                    embeddingJson, 0.45, fetchLimit, formatFilter);
                log.info("[RecEngine][DB] retry returned {} rows", retryRows.size());
                Map<String, Double> retryScores = new LinkedHashMap<>();
                for (Object[] row : retryRows) retryScores.put((String) row[0], ((Number) row[1]).doubleValue());

                before = pool.size();
                resolveItems(retryRows).forEach(item -> addToPool(item, pool, seenKeys));
                log.info("[RecEngine] after low-threshold retry → pool={}", pool.size());

                List<Map<String, Object>> retryHits = pool.values().stream()
                    .filter(item -> retryScores.containsKey(item.getId().toString()))
                    .limit(8).map(item -> {
                        Map<String, Object> h = new LinkedHashMap<>();
                        h.put("title",    item.getTitle());
                        h.put("score",    String.format("%.3f", retryScores.getOrDefault(item.getId().toString(), 0.0)));
                        h.put("category", item.getCategory() != null ? item.getCategory() : "—");
                        return h;
                    }).toList();
                DebugEvents.emit(debug, sid, "search", "vector_retry",
                    "L1 Vector Retry — threshold=0.45 (sparse results)",
                    Map.of("threshold",    0.45,
                           "sql_returned",  retryRows.size(),
                           "new_in_pool",   pool.size() - before,
                           "pool_size",     pool.size(),
                           "new_hits",      retryHits));
            }
        }

        // ── Level 2: keyword × city/region ────────────────────────────────────
        if (request.getRegion() != null && !request.getRegion().isBlank()) {
            searchByKeywordsTracked(kwr.all(), request.getRegion(), fetchLimit,
                pool, seenKeys, sid, "L2 Keyword × Region", debug);
        }

        // ── Level 3: keyword × country ────────────────────────────────────────
        if (pool.size() < targetLimit && request.getCountry() != null && !request.getCountry().isBlank()) {
            String country = request.getCountry();
            int before3 = pool.size();
            searchByKeywordsTracked(kwr.all(), country, fetchLimit,
                pool, seenKeys, sid, "L3 Keyword × Country", debug);
            if (pool.size() > before3 && request.getRegion() != null && !request.getRegion().isBlank()) {
                request.setRelaxationNote(
                    "No results specific to " + request.getRegion() + " — showing " + country + " media");
            }
        }

        // ── Level 4: keyword × no geo ─────────────────────────────────────────
        if (pool.size() < targetLimit) {
            int before4 = pool.size();
            searchByKeywordsTracked(kwr.all(), "", fetchLimit,
                pool, seenKeys, sid, "L4 Keyword (no geo)", debug);
            if (pool.size() > before4 && (request.getRegion() != null || request.getCountry() != null)) {
                request.setRelaxationNote("Showing top media by traffic — no region-specific results found");
            }
        }

        // ── Level 4a: category padding ─────────────────────────────────────────
        if (pool.size() < targetLimit && request.getCategories() != null) {
            int before4a = pool.size();
            List<Map<String, Object>> catResults = new ArrayList<>();
            for (String cat : request.getCategories()) {
                int bCat = pool.size();
                log.debug("[RecEngine][DB] findByCategory category='{}' limit={}", cat, fetchLimit);
                List<MediaItem> catItems = resolveItems(mediaRepository.findByCategory(cat, fetchLimit));
                log.debug("[RecEngine][DB] findByCategory category='{}' returned {} items", cat, catItems.size());
                catItems.forEach(item -> addToPool(item, pool, seenKeys));
                catResults.add(Map.of(
                    "category", cat,
                    "sql_returned", catItems.size(),
                    "new_in_pool",  pool.size() - bCat,
                    "titles",       catItems.stream().limit(4).map(MediaItem::getTitle).toList()
                ));
                if (pool.size() >= targetLimit) break;
            }
            log.info("[RecEngine] after category padding → pool={}", pool.size());
            DebugEvents.emit(debug, sid, "search", "category_pad",
                "L4a Category Padding",
                Map.of("new_in_pool", pool.size() - before4a,
                       "pool_size",   pool.size(),
                       "per_category", catResults));
        }

        // ── Level 4b: top-traffic padding ─────────────────────────────────────
        if (pool.size() < targetLimit) {
            log.info("[RecEngine] pool={} / target={} — padding with top-traffic items",
                pool.size(), targetLimit);
            int before4b = pool.size();
            log.info("[RecEngine][DB] findTopN limit={}", fetchLimit * 2);
            List<MediaItem> topItems = resolveItems(mediaRepository.findTopN(fetchLimit * 2));
            log.info("[RecEngine][DB] findTopN returned {} items", topItems.size());
            topItems.forEach(item -> addToPool(item, pool, seenKeys));
            DebugEvents.emit(debug, sid, "search", "top_traffic",
                "L4b Top-Traffic Padding (no keyword match found)",
                Map.of("sql_returned", topItems.size(),
                       "new_in_pool",  pool.size() - before4b,
                       "pool_size",    pool.size(),
                       "titles",       topItems.stream().limit(5).map(MediaItem::getTitle).toList()));
        }

        log.info("[RecEngine] {} total candidates (target={})", pool.size(), targetLimit);

        // Final summary with all candidate titles + key data
        List<Map<String, Object>> allCandidates = pool.values().stream().map(item -> {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("title",    item.getTitle());
            c.put("category", item.getCategory() != null ? item.getCategory() : "—");
            c.put("format",   item.getFormatType() != null ? item.getFormatType() : "—");
            if (item.getCostUsd() != null) c.put("cost_usd", item.getCostUsd());
            if (item.getSimilarwebVisits() != null)
                c.put("traffic", item.getSimilarwebVisits() >= 1_000_000
                    ? String.format("%.1fM", item.getSimilarwebVisits() / 1_000_000.0)
                    : item.getSimilarwebVisits() >= 1_000
                        ? String.format("%.0fK", item.getSimilarwebVisits() / 1_000.0)
                        : String.valueOf(item.getSimilarwebVisits()));
            c.put("enriched", item.getDescription() != null && !item.getDescription().isBlank());
            return c;
        }).toList();
        DebugEvents.emit(debug, sid, "search", "final_pool",
            "Final Candidate Pool (" + pool.size() + " → Enrichment LLM)",
            Map.of("total_candidates", pool.size(),
                   "target_results",   targetLimit,
                   "candidates",       allCandidates));
        return new ArrayList<>(pool.values());
    }

    /**
     * Keyword search with per-keyword tracing: emits one debug event per level
     * showing how many items each individual keyword matched.
     */
    private void searchByKeywordsTracked(
            List<String> keywords, String geo, int limit,
            LinkedHashMap<UUID, MediaItem> pool, Set<String> seenKeys,
            String sessionId, String levelLabel,
            Consumer<WebSocketMessage> debug) {

        String region = geo != null ? geo : "";
        int poolBefore = pool.size();
        List<Map<String, Object>> kwRows = new ArrayList<>();

        for (String kw : keywords) {
            if (kw.length() < 4) continue;
            int before = pool.size();
            log.debug("[RecEngine][DB] findByTextAndRegion keyword='{}' region='{}' limit={}", kw, region, limit);
            List<Object[]> rows = mediaRepository.findByTextAndRegion(kw, region, limit);
            log.debug("[RecEngine][DB] findByTextAndRegion keyword='{}' returned {} rows", kw, rows.size());
            List<MediaItem> found = resolveItems(rows);
            found.forEach(item -> addToPool(item, pool, seenKeys));

            Map<String, Object> kwEntry = new LinkedHashMap<>();
            kwEntry.put("keyword",      kw);
            kwEntry.put("sql_returned", rows.size());
            kwEntry.put("new_in_pool",  pool.size() - before);
            kwEntry.put("titles",       found.stream().limit(3).map(MediaItem::getTitle).toList());
            kwRows.add(kwEntry);

            if (pool.size() >= limit) break;
        }

        String geoLabel = region.isBlank() ? "any geo" : region;
        DebugEvents.emit(debug, sessionId, "search",
            levelLabel.toLowerCase().replace(" ", "_"),
            levelLabel + " [" + geoLabel + "]",
            Map.of("geo",         geoLabel,
                   "keywords_tried", kwRows.size(),
                   "new_in_pool", pool.size() - poolBefore,
                   "pool_size",   pool.size(),
                   "per_keyword", kwRows));
    }

    private static void addToPool(MediaItem item, LinkedHashMap<UUID, MediaItem> pool, Set<String> seenKeys) {
        String key = item.getUrl() != null
            ? item.getUrl() + "|" + Objects.toString(item.getFormatType(), "")
            : item.getId().toString();
        if (seenKeys.add(key)) {
            pool.put(item.getId(), item);
        }
    }

    private record KeywordsResult(
        List<String> all,
        List<String> fromCategories,
        List<String> fromKeywords,
        List<String> fromAudience,
        List<String> fromObjective
    ) {}

    private KeywordsResult buildKeywordsDetailed(RecommendationRequestDTO request) {
        List<String> fromCategories = new ArrayList<>();
        List<String> fromKeywords   = new ArrayList<>();
        List<String> fromAudience   = new ArrayList<>();
        List<String> fromObjective  = new ArrayList<>();

        if (request.getCategories() != null)
            fromCategories.addAll(request.getCategories());

        if (request.getKeywords() != null)
            fromKeywords.addAll(request.getKeywords());

        if (request.getTargetAudienceDescription() != null) {
            Arrays.stream(request.getTargetAudienceDescription().split("[\\s,;]+"))
                .map(String::toLowerCase)
                .filter(w -> w.length() > 3)
                .filter(w -> !STOPWORDS.contains(w))
                .distinct()
                .limit(3)
                .forEach(fromAudience::add);
        }

        if (request.getCampaignObjective() != null && !request.getCampaignObjective().isBlank())
            fromObjective.add(request.getCampaignObjective());

        List<String> all = new ArrayList<>();
        all.addAll(fromCategories);
        all.addAll(fromKeywords);
        all.addAll(fromAudience);
        all.addAll(fromObjective);
        if (all.isEmpty()) all.add("");

        return new KeywordsResult(all, fromCategories, fromKeywords, fromAudience, fromObjective);
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
        log.debug("[RecEngine][DB] findAllById count={} ids_preview={}",
            ids.size(), ids.stream().limit(3).map(UUID::toString).toList());
        Map<UUID, MediaItem> byId = mediaRepository.findAllById(ids).stream()
            .collect(Collectors.toMap(MediaItem::getId, Function.identity()));
        log.debug("[RecEngine][DB] findAllById resolved {}/{} items", byId.size(), ids.size());
        // Preserve the original order (sorted by traffic from SQL)
        return ids.stream()
            .map(byId::get)
            .filter(Objects::nonNull)
            .toList();
    }
}
