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
            .doOnNext(items -> log.info("[RecEngine] {} items for session={}", items.size(), request.getSessionId()))
            .doOnError(e -> log.error("[RecEngine] query failed: {}", e.getMessage(), e));
    }

    private List<MediaItem> sqlQuery(RecommendationRequestDTO request) {
        int limit = request.getMaxResults() > 0 ? request.getMaxResults() : 10;
        String region = request.getRegion() != null ? request.getRegion().trim() : "";

        // Try category + region combination first
        if (request.getCategories() != null && !request.getCategories().isEmpty()) {
            for (String category : request.getCategories()) {
                String keyword = mapCategoryKeyword(category);
                List<Object[]> rows = mediaRepository.findByTextAndRegion(keyword, region, limit);
                if (!rows.isEmpty()) {
                    log.info("[RecEngine] category='{}' region='{}' → {} results", category, region, rows.size());
                    return resolveItems(rows);
                }
            }
        }

        // Try audience description + region
        if (request.getTargetAudienceDescription() != null
                && !request.getTargetAudienceDescription().isBlank()) {
            String keyword = firstWord(request.getTargetAudienceDescription());
            List<Object[]> rows = mediaRepository.findByTextAndRegion(keyword, region, limit);
            if (!rows.isEmpty()) {
                log.info("[RecEngine] audience='{}' region='{}' → {} results", keyword, region, rows.size());
                return resolveItems(rows);
            }
        }

        // Geographic relaxation: try broader region
        if (!region.isBlank()) {
            List<MediaItem> relaxed = findWithGeoRelaxation(region, limit, request);
            if (!relaxed.isEmpty()) return relaxed;
        }

        // Last resort: top-N newest items
        log.warn("[RecEngine] no match — returning top-N");
        return resolveItems(mediaRepository.findTopN(limit));
    }

    private List<MediaItem> findWithGeoRelaxation(String region, int limit, RecommendationRequestDTO request) {
        String lowerRegion = region.toLowerCase();

        // Step 1: broaden city → oblast
        String oblast = CITY_TO_OBLAST.get(lowerRegion);
        if (oblast != null) {
            String keyword = request.getCategories() != null && !request.getCategories().isEmpty()
                ? mapCategoryKeyword(request.getCategories().get(0))
                : "";
            List<Object[]> rows = mediaRepository.findByTextAndRegion(keyword, oblast, limit);
            if (!rows.isEmpty()) {
                log.info("[RecEngine] geo relaxation: '{}' → '{}' → {} results", region, oblast, rows.size());
                request.setRelaxationNote("No results in " + region + " — showing " + oblast + " coverage");
                return resolveItems(rows);
            }
        }

        // Step 2: national reach items
        List<Object[]> national = mediaRepository.findByReachTier("national", limit);
        if (!national.isEmpty()) {
            log.info("[RecEngine] geo relaxation: '{}' → national reach → {} results", region, national.size());
            request.setRelaxationNote("No regional results for " + region + " — showing national-reach media");
            return resolveItems(national);
        }

        return List.of();
    }

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

    private String firstWord(String text) {
        if (text == null || text.isBlank()) return "";
        String trimmed = text.trim();
        return trimmed.contains(" ") ? trimmed.substring(0, trimmed.indexOf(' ')) : trimmed;
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
