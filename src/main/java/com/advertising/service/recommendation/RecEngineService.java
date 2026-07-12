package com.advertising.service.recommendation;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import com.advertising.service.openai.OpenAIService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecEngineService {

    private final MediaRepository mediaRepository;
    private final OpenAIService openAIService;

    private static final double VECTOR_THRESHOLD = 0.60;

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request) {
        String queryText = buildQueryText(request);
        return openAIService.createEmbedding(queryText)
            .map(RecEngineService::vectorToJson)
            .onErrorResume(e -> {
                log.warn("[RecEngine] embedding failed, proceeding without vector search: {}", e.getMessage());
                return Mono.just("");
            })
            .flatMap(embeddingJson -> Mono.fromCallable(() -> sqlQuery(request, embeddingJson))
                .subscribeOn(Schedulers.boundedElastic()))
            .doOnNext(items -> log.info("[RecEngine] {} candidates for session={}", items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] query failed: {}", e.getMessage(), e));
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

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request, String embeddingJson) {
        int targetLimit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;
        int fetchLimit  = Math.max(targetLimit * 4, 40);

        List<String> keywords = buildKeywords(request);
        log.info("[RecEngine] keywords={} region='{}' country='{}' fetchLimit={}",
            keywords, request.getRegion(), request.getCountry(), fetchLimit);

        // Dedup key: url + "|" + formatType prevents same outlet×format appearing twice
        // (defense against duplicate CSV rows). Falls back to UUID string when url is null.
        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();
        Set<String> seenKeys = new HashSet<>();

        // ── Level 1: vector similarity search (semantic, enriched items only) ──
        if (!embeddingJson.isBlank()) {
            // Apply format pre-filter at SQL level when user specified a preference
            String formatFilter = request.getFormatPreference() != null
                ? request.getFormatPreference() : "";
            resolveItems(mediaRepository.findSimilarByEmbeddingFiltered(
                    embeddingJson, VECTOR_THRESHOLD, fetchLimit, formatFilter))
                .forEach(item -> addToPool(item, pool, seenKeys));
            log.info("[RecEngine] after vector search (threshold={}, format='{}') → pool={}",
                VECTOR_THRESHOLD, formatFilter.isEmpty() ? "any" : formatFilter, pool.size());

            // Retry at lower threshold when results are sparse
            if (pool.size() < targetLimit / 2) {
                log.info("[RecEngine] sparse vector results, retrying at threshold=0.45");
                resolveItems(mediaRepository.findSimilarByEmbeddingFiltered(
                        embeddingJson, 0.45, fetchLimit, formatFilter))
                    .forEach(item -> addToPool(item, pool, seenKeys));
                log.info("[RecEngine] after low-threshold retry → pool={}", pool.size());
            }
        }

        // ── Level 2: keyword × city/region ────────────────────────────────────
        searchByKeywords(keywords, request.getRegion(), fetchLimit, pool, seenKeys);

        // ── Level 3: keyword × country (if region gave too few) ──────────────
        if (pool.size() < targetLimit && request.getCountry() != null && !request.getCountry().isBlank()) {
            String country = request.getCountry();
            searchByKeywords(keywords, country, fetchLimit, pool, seenKeys);
            if (!pool.isEmpty() && request.getRegion() != null && !request.getRegion().isBlank()) {
                request.setRelaxationNote(
                    "No results specific to " + request.getRegion() + " — showing " + country + " media");
            }
        }

        // ── Level 4: keyword × no geo (if still thin) ─────────────────────────
        if (pool.size() < targetLimit) {
            searchByKeywords(keywords, "", fetchLimit, pool, seenKeys);
            if (!pool.isEmpty() && (request.getRegion() != null || request.getCountry() != null)) {
                request.setRelaxationNote("Showing top media by traffic — no region-specific results found");
            }
        }

        // ── Level 4a: pad with enriched items matching the requested category ───
        if (pool.size() < targetLimit && request.getCategories() != null) {
            for (String cat : request.getCategories()) {
                resolveItems(mediaRepository.findByCategory(cat, fetchLimit))
                    .forEach(item -> addToPool(item, pool, seenKeys));
                if (pool.size() >= targetLimit) break;
            }
            log.info("[RecEngine] after category padding → pool={}", pool.size());
        }

        // ── Level 4b: fill remaining slots with top-traffic items ─────────────
        if (pool.size() < targetLimit) {
            log.info("[RecEngine] pool={} / target={} — padding with top-traffic items",
                pool.size(), targetLimit);
            resolveItems(mediaRepository.findTopN(fetchLimit * 2))
                .forEach(item -> addToPool(item, pool, seenKeys));
        }

        log.info("[RecEngine] {} total candidates (target={})", pool.size(), targetLimit);
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
