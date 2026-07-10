package com.advertising.service.recommendation;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.repository.MediaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecEngineService {

    private final MediaRepository mediaRepository;

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request) {
        return Mono.fromCallable(() -> sqlQuery(request))
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(items -> log.info("[RecEngine] {} candidates for session={}", items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] query failed: {}", e.getMessage(), e));
    }

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request) {
        int targetLimit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;
        int fetchLimit  = Math.min(targetLimit * 3, 30);

        List<String> keywords = buildKeywords(request);
        log.info("[RecEngine] keywords={} region='{}' country='{}' fetchLimit={}",
            keywords, request.getRegion(), request.getCountry(), fetchLimit);

        // ── Level 1: keyword × city/region ────────────────────────────────────
        LinkedHashMap<UUID, MediaItem> pool = searchByKeywords(keywords, request.getRegion(), fetchLimit);

        // ── Level 2: keyword × country (if region gave too few) ───────────────
        if (pool.size() < targetLimit && request.getCountry() != null && !request.getCountry().isBlank()) {
            String country = request.getCountry();
            searchByKeywords(keywords, country, fetchLimit)
                .forEach((id, item) -> pool.putIfAbsent(id, item));

            if (pool.size() > 0 && request.getRegion() != null && !request.getRegion().isBlank()) {
                request.setRelaxationNote(
                    "No results specific to " + request.getRegion() + " — showing " + country + " media");
            }
        }

        // ── Level 3: keyword × no geo (if still thin) ─────────────────────────
        if (pool.size() < targetLimit) {
            searchByKeywords(keywords, "", fetchLimit)
                .forEach((id, item) -> pool.putIfAbsent(id, item));

            if (pool.size() > 0 && (request.getRegion() != null || request.getCountry() != null)) {
                request.setRelaxationNote("Showing top media by traffic — no region-specific results found");
            }
        }

        // ── Absolute fallback: top by traffic ──────────────────────────────────
        if (pool.isEmpty()) {
            log.warn("[RecEngine] no keyword matches — falling back to top-N by traffic");
            resolveItems(mediaRepository.findTopN(fetchLimit))
                .forEach(item -> pool.putIfAbsent(item.getId(), item));
        }

        log.info("[RecEngine] {} total candidates (target={})", pool.size(), targetLimit);
        return new ArrayList<>(pool.values());
    }

    private LinkedHashMap<UUID, MediaItem> searchByKeywords(
            List<String> keywords, String geo, int limit) {

        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();
        String region = geo != null ? geo : "";

        for (String kw : keywords) {
            List<Object[]> rows = mediaRepository.findByTextAndRegion(kw, region, limit);
            resolveItems(rows).forEach(item -> pool.putIfAbsent(item.getId(), item));
            if (pool.size() >= limit) break;
        }
        return pool;
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
        "men", "women", "aged", "tech", "savvy", "based", "interested"
    );

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
