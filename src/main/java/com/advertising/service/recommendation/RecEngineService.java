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
import java.util.LinkedHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecEngineService {

    private final MediaRepository mediaRepository;

    // City → Oblast (Ukrainian geography relaxation)
    private static final Map<String, String> CITY_TO_OBLAST = Map.ofEntries(
        Map.entry("uzhhorod", "Zakarpattia Oblast"),
        Map.entry("ужгород", "Закарпаття"),
        Map.entry("lviv", "Lviv Oblast"),
        Map.entry("львів", "Львівщина"),
        Map.entry("kharkiv", "Kharkiv Oblast"),
        Map.entry("харків", "Харківщина"),
        Map.entry("dnipro", "Dnipropetrovsk Oblast"),
        Map.entry("дніпро", "Дніпропетровщина"),
        Map.entry("odesa", "Odesa Oblast"),
        Map.entry("одеса", "Одещина"),
        Map.entry("zaporizhzhia", "Zaporizhzhia Oblast"),
        Map.entry("запоріжжя", "Запорізька область"),
        Map.entry("vinnytsia", "Vinnytsia Oblast"),
        Map.entry("вінниця", "Вінниччина"),
        Map.entry("poltava", "Poltava Oblast"),
        Map.entry("полтава", "Полтавщина"),
        Map.entry("sumy", "Sumy Oblast"),
        Map.entry("суми", "Сумщина"),
        Map.entry("chernihiv", "Chernihiv Oblast"),
        Map.entry("чернігів", "Чернігівщина"),
        Map.entry("rivne", "Rivne Oblast"),
        Map.entry("рівне", "Рівненщина"),
        Map.entry("lutsk", "Volyn Oblast"),
        Map.entry("луцьк", "Волинь"),
        Map.entry("ivano-frankivsk", "Ivano-Frankivsk Oblast"),
        Map.entry("івано-франківськ", "Прикарпаття"),
        Map.entry("ternopil", "Ternopil Oblast"),
        Map.entry("тернопіль", "Тернопільщина"),
        Map.entry("chernivtsi", "Chernivtsi Oblast"),
        Map.entry("чернівці", "Буковина"),
        Map.entry("khmelnytskyi", "Khmelnytskyi Oblast"),
        Map.entry("хмельницький", "Хмельниччина"),
        Map.entry("zhytomyr", "Zhytomyr Oblast"),
        Map.entry("житомир", "Житомирщина"),
        Map.entry("kropyvnytskyi", "Kirovohrad Oblast"),
        Map.entry("кропивницький", "Кіровоградщина"),
        Map.entry("mykolaiv", "Mykolaiv Oblast"),
        Map.entry("миколаїв", "Миколаївщина"),
        Map.entry("kherson", "Kherson Oblast"),
        Map.entry("херсон", "Херсонщина")
    );

    public Mono<List<MediaItem>> findCandidates(RecommendationRequestDTO request) {
        return Mono.fromCallable(() -> sqlQuery(request))
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(items -> log.info("[RecEngine] {} candidates for session={}", items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] query failed: {}", e.getMessage(), e));
    }

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request) {
        int targetLimit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;
        // Over-fetch so the enrichment LLM can filter to the best N
        int fetchLimit = Math.min(targetLimit * 3, 30);
        String region = request.getRegion() != null ? request.getRegion().trim() : "";

        // Build keyword list: all categories + meaningful audience words
        List<String> keywords = buildKeywords(request);
        log.info("[RecEngine] keywords={} region='{}' fetchLimit={}", keywords, region, fetchLimit);

        // Collect candidates from ALL keywords (merge + deduplicate by ID)
        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();

        for (String kw : keywords) {
            List<Object[]> rows = mediaRepository.findByTextAndRegion(kw, region, fetchLimit);
            resolveItems(rows).forEach(item -> pool.putIfAbsent(item.getId(), item));
            if (pool.size() >= fetchLimit) break;
        }

        // If region-specific pool is thin → try geo relaxation to fill up
        if (!region.isBlank() && pool.size() < targetLimit) {
            List<MediaItem> relaxed = findWithGeoRelaxation(region, fetchLimit, request, keywords);
            relaxed.forEach(item -> pool.putIfAbsent(item.getId(), item));
        }

        // Absolute fallback
        if (pool.isEmpty()) {
            log.warn("[RecEngine] no match for any keyword — falling back to top-N");
            resolveItems(mediaRepository.findTopN(fetchLimit)).forEach(item -> pool.putIfAbsent(item.getId(), item));
        }

        List<MediaItem> result = new ArrayList<>(pool.values());
        log.info("[RecEngine] {} total candidates (target={})", result.size(), targetLimit);
        return result;
    }

    private List<MediaItem> findWithGeoRelaxation(String region, int limit,
                                                    RecommendationRequestDTO request,
                                                    List<String> keywords) {
        String lowerRegion = region.toLowerCase();
        LinkedHashMap<UUID, MediaItem> pool = new LinkedHashMap<>();

        // Step 1: broaden city → oblast
        String oblast = CITY_TO_OBLAST.get(lowerRegion);
        if (oblast != null) {
            for (String kw : keywords) {
                resolveItems(mediaRepository.findByTextAndRegion(kw, oblast, limit))
                    .forEach(item -> pool.putIfAbsent(item.getId(), item));
            }
            if (!pool.isEmpty()) {
                log.info("[RecEngine] geo relaxation: '{}' → '{}' → {} results", region, oblast, pool.size());
                request.setRelaxationNote("No results in " + region + " — showing " + oblast + " coverage");
                return new ArrayList<>(pool.values());
            }
        }

        // Step 2: national reach items (merged with keyword pool)
        List<Object[]> national = mediaRepository.findByReachTier("national", limit);
        resolveItems(national).forEach(item -> pool.putIfAbsent(item.getId(), item));
        if (!pool.isEmpty()) {
            log.info("[RecEngine] geo relaxation: '{}' → national reach → {} results", region, pool.size());
            request.setRelaxationNote("No regional results for " + region + " — showing national-reach media");
            return new ArrayList<>(pool.values());
        }

        return List.of();
    }

    private List<String> buildKeywords(RecommendationRequestDTO request) {
        List<String> keywords = new ArrayList<>();

        // Primary: mapped category names
        if (request.getCategories() != null) {
            request.getCategories().stream()
                .map(this::mapCategoryKeyword)
                .forEach(keywords::add);
        }

        // Secondary: meaningful words from audience description (skip stopwords)
        if (request.getTargetAudienceDescription() != null) {
            Arrays.stream(request.getTargetAudienceDescription().split("[\\s,;]+"))
                .map(String::toLowerCase)
                .filter(w -> w.length() > 3)
                .filter(w -> !AUDIENCE_STOPWORDS.contains(w))
                .distinct()
                .limit(4)
                .forEach(keywords::add);
        }

        // Tertiary: campaign objective
        if (request.getCampaignObjective() != null && !request.getCampaignObjective().isBlank()) {
            keywords.add(request.getCampaignObjective());
        }

        // Fallback: empty string = match all
        if (keywords.isEmpty()) keywords.add("");
        return keywords;
    }

    private static final Set<String> AUDIENCE_STOPWORDS = Set.of(
        "the", "and", "for", "with", "that", "this", "from", "are", "who",
        "ages", "age", "primarily", "their", "have", "been", "they", "will",
        "target", "audience", "demographic", "people", "users", "customers"
    );

    private String mapCategoryKeyword(String category) {
        return switch (category.toLowerCase()) {
            case "technology", "software", "it", "tech" -> "Technology";
            case "business", "b2b", "finance"            -> "Business";
            case "news"                                   -> "News";
            case "sport", "sports"                        -> "Sports";
            case "fashion", "lifestyle"                   -> "Fashion";
            case "video", "tv"                            -> "Video";
            case "agriculture", "agro"                    -> "Agriculture";
            case "entertainment"                          -> "Entertainment";
            case "science"                                -> "Science";
            case "politics"                               -> "Politics";
            default -> category;
        };
    }

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
