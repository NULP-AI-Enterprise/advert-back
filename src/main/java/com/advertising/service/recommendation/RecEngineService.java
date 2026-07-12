package com.advertising.service.recommendation;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.repository.MediaRepository;
import com.advertising.service.debug.DebugEvents;
import com.advertising.service.openai.OpenAIService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecEngineService {

    private final MediaRepository mediaRepository;
    private final OpenAIService openAIService;

    private static final double VECTOR_THRESHOLD      = 0.60;
    private static final double VECTOR_RETRY_THRESHOLD = 0.45;

    // Per-SQL-query row limit — how many rows each individual DB call may return.
    // Kept at 100 so queries stay fast and memory stays bounded.
    private static final int PER_QUERY_LIMIT = 100;

    // Flag so catalog stats are printed once per JVM start, not once per request.
    private final AtomicBoolean dbStatsPrinted = new AtomicBoolean(false);

    @PostConstruct
    void logDbStatsAtStartup() {
        // Defer to avoid blocking the main Spring context thread.
        Schedulers.boundedElastic().schedule(() -> {
            try {
                long total      = mediaRepository.countAll();
                long embedded   = mediaRepository.countWithEmbedding();
                long enriched   = mediaRepository.countEnriched();
                long categories = mediaRepository.countDistinctCategories();
                log.info("[RecEngine][CATALOG] total={} with_embedding={} ({}%) enriched={} ({}%) distinct_categories={}",
                    total,
                    embedded, total > 0 ? String.format("%.1f", 100.0 * embedded / total) : "0.0",
                    enriched, total > 0 ? String.format("%.1f", 100.0 * enriched / total) : "0.0",
                    categories);
                dbStatsPrinted.set(true);
            } catch (Exception e) {
                log.warn("[RecEngine][CATALOG] could not fetch DB stats at startup: {}", e.getMessage());
            }
        });
    }

    // ── Public API ──────────────────────────────────────────────────────────────

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request,
                                                 Consumer<WebSocketMessage> debug) {
        String queryText = buildQueryText(request);
        log.info("[RecEngine] findCandidates START session={} query_text='{}' maxResults={}",
            request.getSessionId(), queryText, request.getMaxResults());

        DebugEvents.emit(debug, request.getSessionId(), "search", "embed_query",
            "Embedding Query → OpenAI",
            Map.of("query_text",  queryText,
                   "region",      request.getRegion()  != null ? request.getRegion()  : "any",
                   "country",     request.getCountry() != null ? request.getCountry() : "any",
                   "format",      request.getFormatPreference() != null ? request.getFormatPreference() : "any",
                   "max_results", request.getMaxResults()));

        // Log catalog coverage once per startup so we know how many items are searchable.
        if (!dbStatsPrinted.get()) {
            try {
                long total    = mediaRepository.countAll();
                long embedded = mediaRepository.countWithEmbedding();
                long enriched = mediaRepository.countEnriched();
                log.info("[RecEngine][CATALOG] total={} with_embedding={} ({}%) enriched={}",
                    total, embedded,
                    total > 0 ? String.format("%.1f", 100.0 * embedded / total) : "0.0",
                    enriched);
                dbStatsPrinted.set(true);
            } catch (Exception e) {
                log.warn("[RecEngine][CATALOG] DB stats unavailable: {}", e.getMessage());
            }
        }

        return openAIService.createEmbedding(queryText)
            .map(RecEngineService::vectorToJson)
            .onErrorResume(e -> {
                log.warn("[RecEngine] embedding failed — proceeding without vector search: {}", e.getMessage());
                DebugEvents.emit(debug, request.getSessionId(), "search", "embed_error",
                    "Embedding Failed — skipping vector search", Map.of("error", e.getMessage()));
                return Mono.just("");
            })
            .flatMap(embeddingJson -> Mono.fromCallable(() -> sqlQuery(request, embeddingJson, debug))
                .subscribeOn(Schedulers.boundedElastic()))
            .doOnNext(items -> log.info("[RecEngine] findCandidates END → {} candidates session={}",
                items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] query failed: {}", e.getMessage(), e));
    }

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request) {
        return findCandidates(request, DebugEvents.NOOP);
    }

    // ── Core search logic ───────────────────────────────────────────────────────

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request, String embeddingJson,
                                     Consumer<WebSocketMessage> debug) {
        int targetLimit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;
        // poolCap: how many candidates to accumulate for the enrichment LLM.
        // Larger pool = better scoring diversity but more tokens sent to GPT.
        // 15× target gives 150 candidates for maxResults=10 (vs old hard cap of 40).
        int poolCap = Math.max(targetLimit * 15, 150);
        String sid  = request.getSessionId();

        log.info("[RecEngine] sqlQuery START: targetLimit={} PER_QUERY_LIMIT={} poolCap={} session={}",
            targetLimit, PER_QUERY_LIMIT, poolCap, sid);

        // ── Build keyword list with source breakdown ───────────────────────────
        KeywordsResult kwr = buildKeywordsDetailed(request);
        log.info("[RecEngine] keywords built: total={} from_categories={} from_llm_keywords={} from_audience={} from_objective={}",
            kwr.all().size(),
            kwr.fromCategories(),
            kwr.fromKeywords(),
            kwr.fromAudience(),
            kwr.fromObjective());
        log.info("[RecEngine] full keyword list: {}", kwr.all());
        log.info("[RecEngine] search params: region='{}' country='{}' format='{}' budget={}",
            request.getRegion(), request.getCountry(),
            request.getFormatPreference(),
            request.getBudgetUsd() != null ? "$" + request.getBudgetUsd() : "not set");

        Map<String, Object> kwDebug = new LinkedHashMap<>();
        kwDebug.put("final_keyword_list",  kwr.all());
        kwDebug.put("from_categories",     kwr.fromCategories());
        kwDebug.put("from_llm_keywords",   kwr.fromKeywords());
        kwDebug.put("from_audience_text",  kwr.fromAudience());
        kwDebug.put("from_objective",      kwr.fromObjective());
        kwDebug.put("per_query_limit",     PER_QUERY_LIMIT);
        kwDebug.put("pool_cap",            poolCap);
        kwDebug.put("target_results",      targetLimit);
        DebugEvents.emit(debug, sid, "search", "keywords_built",
            "Keywords Built (" + kwr.all().size() + " terms from 4 sources)", kwDebug);

        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();
        Set<String> seenKeys = new HashSet<>();

        // Source counters for final breakdown log
        int countVector = 0, countFts = 0, countCategory = 0, countTopTraffic = 0;

        // ── Level 1: vector similarity search ─────────────────────────────────
        if (!embeddingJson.isBlank()) {
            String formatFilter = request.getFormatPreference() != null
                ? request.getFormatPreference() : "";
            int poolBefore = pool.size();

            log.info("[RecEngine][L1][DB] findSimilarByEmbeddingFiltered → threshold={} limit={} format='{}' embedding_dims={}",
                VECTOR_THRESHOLD, PER_QUERY_LIMIT,
                formatFilter.isEmpty() ? "any" : formatFilter,
                embeddingJson.split(",").length);

            List<Object[]> vecRows = mediaRepository.findSimilarByEmbeddingFiltered(
                embeddingJson, VECTOR_THRESHOLD, PER_QUERY_LIMIT, formatFilter);

            log.info("[RecEngine][L1][DB] findSimilarByEmbeddingFiltered ← {} rows returned (threshold={} format='{}')",
                vecRows.size(), VECTOR_THRESHOLD, formatFilter.isEmpty() ? "any" : formatFilter);

            if (vecRows.isEmpty()) {
                log.warn("[RecEngine][L1] 0 vector results — check: (1) do items have embeddings? " +
                    "(2) is threshold={} too high? (3) are ivfflat.probes set? " +
                    "(see CATALOG log above for embedding coverage)", VECTOR_THRESHOLD);
            }

            // Build id→score map before resolving
            Map<String, Double> scores = new LinkedHashMap<>();
            for (Object[] row : vecRows) {
                scores.put((String) row[0], ((Number) row[1]).doubleValue());
            }

            resolveItems(vecRows).forEach(item -> addToPool(item, pool, seenKeys));
            countVector = pool.size() - poolBefore;
            log.info("[RecEngine][L1] after vector search → new_in_pool={} pool={}/{}",
                countVector, pool.size(), poolCap);

            // Log top similarity scores
            if (!scores.isEmpty()) {
                String topScoresStr = scores.entrySet().stream().limit(5)
                    .map(e -> e.getKey().substring(0, 8) + "→" + String.format("%.3f", e.getValue()))
                    .collect(Collectors.joining(", "));
                log.debug("[RecEngine][L1] top similarity scores: {}", topScoresStr);
            }

            // Show top hits with similarity scores in debug panel
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
                Map.of("threshold",      VECTOR_THRESHOLD,
                       "format_filter",  formatFilter.isEmpty() ? "any" : formatFilter,
                       "sql_returned",   vecRows.size(),
                       "new_in_pool",    countVector,
                       "pool_size",      pool.size(),
                       "pool_cap",       poolCap,
                       "top_hits",       topHits));

            // Retry at lower threshold when sparse
            if (pool.size() < targetLimit / 2) {
                poolBefore = pool.size();
                log.info("[RecEngine][L1-retry] sparse vector results (pool={}), retrying at threshold={}",
                    pool.size(), VECTOR_RETRY_THRESHOLD);
                log.info("[RecEngine][L1-retry][DB] findSimilarByEmbeddingFiltered → threshold={} limit={} format='{}'",
                    VECTOR_RETRY_THRESHOLD, PER_QUERY_LIMIT, formatFilter.isEmpty() ? "any" : formatFilter);

                List<Object[]> retryRows = mediaRepository.findSimilarByEmbeddingFiltered(
                    embeddingJson, VECTOR_RETRY_THRESHOLD, PER_QUERY_LIMIT, formatFilter);

                log.info("[RecEngine][L1-retry][DB] ← {} rows returned at threshold={}",
                    retryRows.size(), VECTOR_RETRY_THRESHOLD);

                Map<String, Double> retryScores = new LinkedHashMap<>();
                for (Object[] row : retryRows) retryScores.put((String) row[0], ((Number) row[1]).doubleValue());

                resolveItems(retryRows).forEach(item -> addToPool(item, pool, seenKeys));
                int newFromRetry = pool.size() - poolBefore;
                countVector += newFromRetry;
                log.info("[RecEngine][L1-retry] after retry → new_in_pool={} pool={}/{}",
                    newFromRetry, pool.size(), poolCap);

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
                    "L1 Vector Retry — threshold=" + VECTOR_RETRY_THRESHOLD + " (sparse results)",
                    Map.of("threshold",   VECTOR_RETRY_THRESHOLD,
                           "sql_returned", retryRows.size(),
                           "new_in_pool",  newFromRetry,
                           "pool_size",    pool.size(),
                           "new_hits",     retryHits));
            }
        } else {
            log.info("[RecEngine][L1] skipped — no embedding available");
        }

        // ── Level 2: keyword × city/region ────────────────────────────────────
        if (request.getRegion() != null && !request.getRegion().isBlank()) {
            int before = pool.size();
            log.info("[RecEngine][L2] keyword×region search: region='{}' pool={}/{} keywords={}",
                request.getRegion(), pool.size(), poolCap, kwr.all());
            searchByKeywordsTracked(kwr.all(), request.getRegion(), PER_QUERY_LIMIT, poolCap,
                pool, seenKeys, sid, "L2 Keyword × Region", debug);
            int added = pool.size() - before;
            countFts += added;
            log.info("[RecEngine][L2] after keyword×region → new_in_pool={} pool={}/{}", added, pool.size(), poolCap);
        } else {
            log.debug("[RecEngine][L2] skipped — no region specified");
        }

        // ── Level 3: keyword × country ────────────────────────────────────────
        if (pool.size() < poolCap && request.getCountry() != null && !request.getCountry().isBlank()) {
            int before = pool.size();
            String country = request.getCountry();
            log.info("[RecEngine][L3] keyword×country search: country='{}' pool={}/{}", country, pool.size(), poolCap);
            searchByKeywordsTracked(kwr.all(), country, PER_QUERY_LIMIT, poolCap,
                pool, seenKeys, sid, "L3 Keyword × Country", debug);
            int added = pool.size() - before;
            countFts += added;
            log.info("[RecEngine][L3] after keyword×country → new_in_pool={} pool={}/{}", added, pool.size(), poolCap);
            if (added > 0 && request.getRegion() != null && !request.getRegion().isBlank()) {
                request.setRelaxationNote(
                    "No results specific to " + request.getRegion() + " — showing " + country + " media");
            }
        } else if (pool.size() >= poolCap) {
            log.info("[RecEngine][L3] skipped — pool already full ({}/{})", pool.size(), poolCap);
        } else {
            log.debug("[RecEngine][L3] skipped — no country specified");
        }

        // ── Level 4: keyword × no geo ─────────────────────────────────────────
        if (pool.size() < poolCap) {
            int before = pool.size();
            log.info("[RecEngine][L4] keyword (no geo) search: pool={}/{}", pool.size(), poolCap);
            searchByKeywordsTracked(kwr.all(), "", PER_QUERY_LIMIT, poolCap,
                pool, seenKeys, sid, "L4 Keyword (no geo)", debug);
            int added = pool.size() - before;
            countFts += added;
            log.info("[RecEngine][L4] after keyword(no geo) → new_in_pool={} pool={}/{}", added, pool.size(), poolCap);
            if (added > 0 && (request.getRegion() != null || request.getCountry() != null)) {
                request.setRelaxationNote("Showing top media by traffic — no region-specific results found");
            }
        } else {
            log.info("[RecEngine][L4] skipped — pool full ({}/{})", pool.size(), poolCap);
        }

        // ── Level 4a: category padding ─────────────────────────────────────────
        if (pool.size() < poolCap && request.getCategories() != null && !request.getCategories().isEmpty()) {
            int before4a = pool.size();
            log.info("[RecEngine][L4a] category padding: pool={}/{} categories={}",
                pool.size(), poolCap, request.getCategories());
            List<Map<String, Object>> catResults = new ArrayList<>();
            for (String cat : request.getCategories()) {
                int bCat = pool.size();
                log.debug("[RecEngine][L4a][DB] findByCategory → category='{}' limit={}", cat, PER_QUERY_LIMIT);
                List<MediaItem> catItems = resolveItems(mediaRepository.findByCategory(cat, PER_QUERY_LIMIT));
                log.debug("[RecEngine][L4a][DB] findByCategory ← {} items for category='{}'", catItems.size(), cat);
                catItems.forEach(item -> addToPool(item, pool, seenKeys));
                int added = pool.size() - bCat;
                catResults.add(Map.of(
                    "category",    cat,
                    "sql_returned", catItems.size(),
                    "new_in_pool", added,
                    "titles",      catItems.stream().limit(4).map(MediaItem::getTitle).toList()
                ));
                log.debug("[RecEngine][L4a] category='{}' sql={} new={}", cat, catItems.size(), added);
                if (pool.size() >= poolCap) break;
            }
            int totalAdded = pool.size() - before4a;
            countCategory = totalAdded;
            log.info("[RecEngine][L4a] after category padding → new_in_pool={} pool={}/{}", totalAdded, pool.size(), poolCap);
            DebugEvents.emit(debug, sid, "search", "category_pad",
                "L4a Category Padding",
                Map.of("new_in_pool",  totalAdded,
                       "pool_size",    pool.size(),
                       "pool_cap",     poolCap,
                       "per_category", catResults));
        } else if (pool.size() >= poolCap) {
            log.info("[RecEngine][L4a] skipped — pool full ({}/{})", pool.size(), poolCap);
        }

        // ── Level 4b: top-traffic padding ─────────────────────────────────────
        if (pool.size() < poolCap) {
            int before4b = pool.size();
            int topNLimit = poolCap * 2;
            log.info("[RecEngine][L4b] top-traffic padding: pool={}/{}", pool.size(), poolCap);
            log.info("[RecEngine][L4b][DB] findTopN → limit={}", topNLimit);
            List<MediaItem> topItems = resolveItems(mediaRepository.findTopN(topNLimit));
            log.info("[RecEngine][L4b][DB] findTopN ← {} items returned", topItems.size());
            topItems.forEach(item -> addToPool(item, pool, seenKeys));
            int added = pool.size() - before4b;
            countTopTraffic = added;
            log.info("[RecEngine][L4b] after top-traffic padding → new_in_pool={} pool={}/{}", added, pool.size(), poolCap);
            DebugEvents.emit(debug, sid, "search", "top_traffic",
                "L4b Top-Traffic Padding (no keyword match found)",
                Map.of("sql_returned", topItems.size(),
                       "new_in_pool",  added,
                       "pool_size",    pool.size(),
                       "pool_cap",     poolCap,
                       "titles",       topItems.stream().limit(5).map(MediaItem::getTitle).toList()));
        } else {
            log.info("[RecEngine][L4b] skipped — pool full ({}/{})", pool.size(), poolCap);
        }

        // ── Final summary ──────────────────────────────────────────────────────
        log.info("[RecEngine] POOL COMPLETE: total={} target={} breakdown: vector={} fts={} category={} top_traffic={}",
            pool.size(), targetLimit, countVector, countFts, countCategory, countTopTraffic);

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

        long enrichedCount = allCandidates.stream().filter(c -> Boolean.TRUE.equals(c.get("enriched"))).count();
        log.info("[RecEngine] candidates for enrichment LLM: total={} enriched={} unenriched={}",
            allCandidates.size(), enrichedCount, allCandidates.size() - enrichedCount);

        DebugEvents.emit(debug, sid, "search", "final_pool",
            "Final Candidate Pool (" + pool.size() + " → Enrichment LLM)",
            Map.of("total_candidates", pool.size(),
                   "target_results",   targetLimit,
                   "pool_cap",         poolCap,
                   "breakdown",        Map.of("vector", countVector, "fts", countFts,
                                               "category", countCategory, "top_traffic", countTopTraffic),
                   "enriched_count",   enrichedCount,
                   "candidates",       allCandidates));
        return new ArrayList<>(pool.values());
    }

    // ── Keyword search with per-keyword tracing ─────────────────────────────────

    /**
     * Searches each keyword against the media catalog with geo filter.
     * Accumulates into pool until poolCap is reached.
     *
     * @param perQueryLimit max rows per SQL call
     * @param poolCap       stop adding when pool reaches this size (was previously wrongly capped at fetchLimit=40)
     */
    private void searchByKeywordsTracked(
            List<String> keywords, String geo, int perQueryLimit, int poolCap,
            LinkedHashMap<UUID, MediaItem> pool, Set<String> seenKeys,
            String sessionId, String levelLabel,
            Consumer<WebSocketMessage> debug) {

        String region    = geo != null ? geo : "";
        int poolBefore   = pool.size();
        List<Map<String, Object>> kwRows = new ArrayList<>();
        int kwTried = 0, kwSkipped = 0;

        for (String kw : keywords) {
            if (kw.length() < 4) {
                log.debug("[RecEngine][FTS] skipping short keyword '{}' (len<4)", kw);
                kwSkipped++;
                continue;
            }
            int before = pool.size();
            log.debug("[RecEngine][FTS][DB] findByTextAndRegion → keyword='{}' region='{}' limit={}",
                kw, region.isEmpty() ? "any" : region, perQueryLimit);

            List<Object[]> rows = mediaRepository.findByTextAndRegion(kw, region, perQueryLimit);
            log.debug("[RecEngine][FTS][DB] findByTextAndRegion ← {} rows for keyword='{}'", rows.size(), kw);

            List<MediaItem> found = resolveItems(rows);
            found.forEach(item -> addToPool(item, pool, seenKeys));
            int newItems = pool.size() - before;

            Map<String, Object> kwEntry = new LinkedHashMap<>();
            kwEntry.put("keyword",      kw);
            kwEntry.put("sql_returned", rows.size());
            kwEntry.put("new_in_pool",  newItems);
            kwEntry.put("titles",       found.stream().limit(3).map(MediaItem::getTitle).toList());
            kwRows.add(kwEntry);
            kwTried++;

            if (newItems > 0) {
                log.debug("[RecEngine][FTS] keyword='{}' → sql={} new={} pool={}",
                    kw, rows.size(), newItems, pool.size());
            }

            // Stop when pool is full enough — use poolCap, NOT perQueryLimit
            // (the old bug was: if (pool.size() >= limit) break; where limit = fetchLimit = 40,
            //  so the loop broke after the very first keyword once vector filled the pool to 40)
            if (pool.size() >= poolCap) {
                log.info("[RecEngine][FTS] pool cap reached ({}/{}) after {} keywords — stopping",
                    pool.size(), poolCap, kwTried);
                break;
            }
        }

        String geoLabel = region.isBlank() ? "any geo" : region;
        int totalNew = pool.size() - poolBefore;
        log.info("[RecEngine][FTS] {} [{}] → keywords_tried={} keywords_skipped={} new_in_pool={} pool={}",
            levelLabel, geoLabel, kwTried, kwSkipped, totalNew, pool.size());

        DebugEvents.emit(debug, sessionId, "search",
            levelLabel.toLowerCase().replace(" ", "_"),
            levelLabel + " [" + geoLabel + "]",
            Map.of("geo",             geoLabel,
                   "keywords_tried",  kwTried,
                   "keywords_skipped", kwSkipped,
                   "new_in_pool",     totalNew,
                   "pool_size",       pool.size(),
                   "pool_cap",        poolCap,
                   "per_keyword",     kwRows));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

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

    private static String buildQueryText(RecommendationRequestDTO request) {
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

    private List<MediaItem> resolveItems(List<Object[]> rows) {
        if (rows.isEmpty()) return List.of();
        List<UUID> ids = rows.stream()
            .map(row -> UUID.fromString((String) row[0]))
            .toList();
        log.debug("[RecEngine][DB] findAllById → {} ids", ids.size());
        Map<UUID, MediaItem> byId = mediaRepository.findAllById(ids).stream()
            .collect(Collectors.toMap(MediaItem::getId, Function.identity()));
        log.debug("[RecEngine][DB] findAllById ← {}/{} resolved", byId.size(), ids.size());
        if (byId.size() < ids.size()) {
            log.warn("[RecEngine][DB] findAllById: {} ids not found (stale data?)", ids.size() - byId.size());
        }
        return ids.stream()
            .map(byId::get)
            .filter(Objects::nonNull)
            .toList();
    }

    private static final Set<String> STOPWORDS = Set.of(
        "the", "and", "for", "with", "that", "this", "from", "are", "who",
        "ages", "age", "primarily", "their", "have", "been", "they", "will",
        "target", "audience", "demographic", "people", "users", "customers",
        "men", "women", "aged", "savvy", "based", "interested", "about",
        "looking", "focused", "those", "want", "need", "more", "also"
    );
}
