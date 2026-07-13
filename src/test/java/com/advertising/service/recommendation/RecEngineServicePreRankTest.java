package com.advertising.service.recommendation;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.MediaItem;
import com.advertising.model.websocket.WebSocketMessage;
import com.advertising.repository.MediaRepository;
import com.advertising.service.openai.OpenAIService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@code RecEngineService#preRankAndTrim} via reflection.
 *
 * IMPORTANT: the method only sorts when {@code candidates.size() > maxItems}.
 * For all scoring/ordering tests we therefore supply {@code maxItems = n - 1}
 * (one fewer than the candidate list) so the sorting path is always exercised.
 * The trim-behaviour tests verify the no-trim and trim paths explicitly.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RecEngineService – preRankAndTrim scoring")
class RecEngineServicePreRankTest {

    @Mock
    private MediaRepository mediaRepository;

    @Mock
    private OpenAIService openAIService;

    @InjectMocks
    private RecEngineService recEngineService;

    private Method preRankAndTrim;

    private static final Consumer<WebSocketMessage> NOOP_DEBUG = msg -> {};
    private static final String TEST_SID = "test-session";

    @BeforeEach
    void exposeMethod() throws Exception {
        preRankAndTrim = RecEngineService.class.getDeclaredMethod(
            "preRankAndTrim",
            List.class,
            RecommendationRequestDTO.class,
            int.class,
            Consumer.class,
            String.class
        );
        preRankAndTrim.setAccessible(true);
    }

    @SuppressWarnings("unchecked")
    private List<MediaItem> invoke(List<MediaItem> candidates,
                                   RecommendationRequestDTO request,
                                   int maxItems) throws Exception {
        return (List<MediaItem>) preRankAndTrim.invoke(
            recEngineService, candidates, request, maxItems, NOOP_DEBUG, TEST_SID
        );
    }

    // ─── Builders ─────────────────────────────────────────────────────────────

    private static MediaItem.MediaItemBuilder baseline() {
        return MediaItem.builder()
            .id(UUID.randomUUID())
            .title("Test Outlet");
    }

    private static RecommendationRequestDTO.RecommendationRequestDTOBuilder baseRequest() {
        return RecommendationRequestDTO.builder()
            .sessionId(TEST_SID)
            .maxResults(5);
    }

    /**
     * Helper: a "dummy padding" item with zero score under the given request.
     * Used to force pool > maxItems so sorting fires.
     */
    private static MediaItem dummy(String title) {
        // No category, no visits, no description, no DR/DA, no cost — scores 0 under any request
        return baseline().title(title).build();
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 1. Category signal
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Category scoring")
    class CategoryScoring {

        @Test
        @DisplayName("Category match (+30) ranks higher than mismatch (-10)")
        void categoryMatchBoostAndMismatchPenalty() throws Exception {
            // matching: +30 (category match) + 0 (no traffic) = 30
            // mismatch: -10 (category mismatch)               = -10
            // dummy:     0                                     = 0
            // Pool = 3, maxItems = 2 → sorting fires, dummy dropped
            MediaItem matching   = baseline().title("matching").category("Technology").build();
            MediaItem mismatching = baseline().title("mismatching").category("Sports").build();

            RecommendationRequestDTO request = baseRequest()
                .categories(List.of("Technology"))
                .build();

            // maxItems=2, pool=3 → sorting path forced; dummy dropped last
            List<MediaItem> result = invoke(
                List.of(mismatching, matching, dummy("pad")), request, 2
            );

            assertThat(result).hasSize(2);
            assertThat(result.get(0).getTitle()).isEqualTo("matching");
            assertThat(result.get(1).getTitle()).isEqualTo("pad");
        }

        @Test
        @DisplayName("No category penalty when request.categories is empty")
        void noPenaltyWhenNoCategoriesRequested() throws Exception {
            // Without requested categories the -10 penalty must not fire.
            // Only differentiator is traffic.
            // Pool = 3, maxItems = 2 — sports outlet dropped
            MediaItem highTraffic = baseline().title("high").category("Sports").similarwebVisits(5_000_000L).build();
            MediaItem lowTraffic  = baseline().title("low").category("News").similarwebVisits(50_000L).build();
            MediaItem pad         = dummy("pad");

            RecommendationRequestDTO request = baseRequest().build();

            List<MediaItem> result = invoke(List.of(pad, lowTraffic, highTraffic), request, 2);

            assertThat(result.get(0).getTitle()).isEqualTo("high");
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 2. Traffic tier
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Traffic tier scoring")
    class TrafficTierScoring {

        @Test
        @DisplayName(">5M (+40) > >1M (+30) > >100K (+20) > other (+10)")
        void trafficTiersAreOrderedCorrectly() throws Exception {
            MediaItem tier10 = baseline().title("tier_10").similarwebVisits(50_000L).build();
            MediaItem tier20 = baseline().title("tier_20").similarwebVisits(500_000L).build();
            MediaItem tier30 = baseline().title("tier_30").similarwebVisits(2_000_000L).build();
            MediaItem tier40 = baseline().title("tier_40").similarwebVisits(6_000_000L).build();
            // 5th padding item to force pool > maxItems=4
            MediaItem pad    = dummy("pad");

            RecommendationRequestDTO request = baseRequest().build();

            List<MediaItem> result = invoke(
                List.of(tier10, tier30, tier20, tier40, pad), request, 4
            );

            assertThat(result).hasSize(4);
            assertThat(result).extracting(MediaItem::getTitle)
                .containsExactly("tier_40", "tier_30", "tier_20", "tier_10");
        }

        @Test
        @DisplayName("Null visits (+0) ranks below any item with visits (+10 minimum)")
        void nullVisitsGetsZeroTrafficScore() throws Exception {
            // withVisits: +10 (traffic < 100K)
            // noVisits:    0  (no traffic signal)
            // pad3:        0  (no traffic) — forces sorting with pool=3, maxItems=2
            MediaItem withVisits = baseline().title("with_visits").similarwebVisits(1L).build();
            MediaItem noVisits   = baseline().title("no_visits").similarwebVisits(null).build();
            MediaItem pad        = dummy("pad");

            List<MediaItem> result = invoke(
                List.of(noVisits, withVisits, pad), baseRequest().build(), 2
            );

            assertThat(result.get(0).getTitle()).isEqualTo("with_visits");
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 3. Enrichment signal
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Enrichment scoring")
    class EnrichmentScoring {

        @Test
        @DisplayName("Non-blank description (+20) ranks higher than null description (+0)")
        void enrichedItemScoresHigherThanUnenriched() throws Exception {
            MediaItem enriched   = baseline().title("enriched").description("Detailed editorial.").build();
            MediaItem unenriched = baseline().title("unenriched").description(null).build();
            MediaItem pad        = dummy("pad");

            List<MediaItem> result = invoke(
                List.of(unenriched, enriched, pad), baseRequest().build(), 2
            );

            assertThat(result.get(0).getTitle()).isEqualTo("enriched");
        }

        @Test
        @DisplayName("Blank-only description is treated as unenriched (no +20 bonus)")
        void blankDescriptionIsNotEnriched() throws Exception {
            MediaItem blank    = baseline().title("blank_desc").description("   ").build();
            MediaItem withDesc = baseline().title("with_desc").description("Real description here.").build();
            MediaItem pad      = dummy("pad");

            List<MediaItem> result = invoke(
                List.of(blank, pad, withDesc), baseRequest().build(), 2
            );

            assertThat(result.get(0).getTitle()).isEqualTo("with_desc");
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 4. Budget signal
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Budget scoring")
    class BudgetScoring {

        @Test
        @DisplayName("Within budget (+10) ranks above 3× budget (-20)")
        void withinBudgetRanksAboveTripleBudgetItem() throws Exception {
            double budget = 100.0;
            // in_budget: +10; way_over: -20; pad: 0
            MediaItem inBudget = baseline().title("in_budget").costUsd(BigDecimal.valueOf(80)).build();
            MediaItem wayOver  = baseline().title("way_over").costUsd(BigDecimal.valueOf(350)).build();
            MediaItem pad      = dummy("pad");

            RecommendationRequestDTO request = baseRequest().budgetUsd(budget).build();

            List<MediaItem> result = invoke(List.of(wayOver, inBudget, pad), request, 2);

            assertThat(result.get(0).getTitle()).isEqualTo("in_budget");
        }

        @Test
        @DisplayName("Cost exactly at budget (cost == budget) gets the +10 bonus")
        void exactlyAtBudgetGetsBonus() throws Exception {
            double budget = 200.0;
            // at_budget: cost=200, +10; over_once: cost=250, between budget and 3×budget, 0
            MediaItem atBudget = baseline().title("at_budget").costUsd(BigDecimal.valueOf(200)).build();
            MediaItem overOnce = baseline().title("over_once").costUsd(BigDecimal.valueOf(250)).build();
            MediaItem pad      = dummy("pad");

            RecommendationRequestDTO request = baseRequest().budgetUsd(budget).build();

            List<MediaItem> result = invoke(List.of(overOnce, atBudget, pad), request, 2);

            assertThat(result.get(0).getTitle()).isEqualTo("at_budget");
        }

        @Test
        @DisplayName("No budgetUsd in request means budget signal does not influence order")
        void noBudgetFieldMeansNoBudgetScore() throws Exception {
            // Without budgetUsd, cost fields are ignored.
            // Ordering falls to traffic only.
            MediaItem highCost = baseline().title("expensive").costUsd(BigDecimal.valueOf(9999)).similarwebVisits(5_000_000L).build();
            MediaItem cheap    = baseline().title("cheap").costUsd(BigDecimal.valueOf(1)).similarwebVisits(1_000L).build();
            MediaItem pad      = dummy("pad");

            List<MediaItem> result = invoke(List.of(cheap, pad, highCost), baseRequest().build(), 2);

            assertThat(result.get(0).getTitle()).isEqualTo("expensive");
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 5. Format signal
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Format preference scoring")
    class FormatScoring {

        @Test
        @DisplayName("Case-insensitive format match (+15) ranks above non-matching format item")
        void formatMatchAddsBonus() throws Exception {
            // article: +15; video: 0; pad: 0
            MediaItem articleItem = baseline().title("article").formatType("Article").build();
            MediaItem videoItem   = baseline().title("video").formatType("Video").build();
            MediaItem pad         = dummy("pad");

            RecommendationRequestDTO request = baseRequest()
                .formatPreference("article")   // lowercase — must match case-insensitively
                .build();

            List<MediaItem> result = invoke(List.of(videoItem, articleItem, pad), request, 2);

            assertThat(result.get(0).getTitle()).isEqualTo("article");
        }

        @Test
        @DisplayName("Null formatPreference means no format bonus is applied")
        void nullFormatPreferenceMeansNoBonusApplied() throws Exception {
            MediaItem a   = baseline().title("a").formatType("Article").build();
            MediaItem b   = baseline().title("b").formatType("Video").build();
            MediaItem pad = dummy("pad");

            // All three score the same (0) — confirm no exception and 2 returned
            RecommendationRequestDTO request = baseRequest().formatPreference(null).build();

            List<MediaItem> result = invoke(List.of(a, b, pad), request, 2);

            assertThat(result).hasSize(2);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 6. SEO signals
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("SEO scoring (DR + DA)")
    class SeoScoring {

        @Test
        @DisplayName("DR >= 50 (+10) and DA >= 40 (+5) rank higher than low DR/DA (+0)")
        void highDrAndDaRankHigher() throws Exception {
            // highSeo: +10 (DR=70) + +5 (DA=50) = 15
            // lowSeo:    0                         =  0
            MediaItem highSeo = baseline().title("high_seo").ahrefsDr(70).mozDa(50).build();
            MediaItem lowSeo  = baseline().title("low_seo").ahrefsDr(30).mozDa(20).build();
            MediaItem pad     = dummy("pad");

            List<MediaItem> result = invoke(List.of(lowSeo, highSeo, pad), baseRequest().build(), 2);

            assertThat(result.get(0).getTitle()).isEqualTo("high_seo");
        }

        @Test
        @DisplayName("DR exactly 50 qualifies for the +10 bonus (boundary inclusive)")
        void drExactly50QualifiesForBonus() throws Exception {
            MediaItem dr50 = baseline().title("dr_50").ahrefsDr(50).build();
            MediaItem dr49 = baseline().title("dr_49").ahrefsDr(49).build();
            MediaItem pad  = dummy("pad");

            List<MediaItem> result = invoke(List.of(dr49, dr50, pad), baseRequest().build(), 2);

            assertThat(result.get(0).getTitle()).isEqualTo("dr_50");
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 7. Trim / no-trim behaviour
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Trim behaviour")
    class TrimBehaviour {

        @Test
        @DisplayName("Returns exactly maxItems when pool size > maxItems")
        void trimsToMaxItemsWhenPoolIsLarger() throws Exception {
            List<MediaItem> candidates = buildNItems(20);
            List<MediaItem> result = invoke(candidates, baseRequest().build(), 5);
            assertThat(result).hasSize(5);
        }

        @Test
        @DisplayName("Returns all candidates when pool size < maxItems (no-trim path)")
        void returnsAllWhenPoolFitsWithinMaxItems() throws Exception {
            List<MediaItem> candidates = buildNItems(3);
            List<MediaItem> result = invoke(candidates, baseRequest().build(), 10);
            assertThat(result).hasSize(3);
        }

        @Test
        @DisplayName("Returns all candidates when pool size exactly equals maxItems (no-trim path)")
        void returnsAllWhenPoolExactlyEqualsMaxItems() throws Exception {
            List<MediaItem> candidates = buildNItems(5);
            List<MediaItem> result = invoke(candidates, baseRequest().build(), 5);
            assertThat(result).hasSize(5);
        }

        private List<MediaItem> buildNItems(int n) {
            List<MediaItem> items = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                items.add(baseline().title("item_" + i).build());
            }
            return items;
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 8. Sort order
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Sort order")
    class SortOrder {

        @Test
        @DisplayName("Result is sorted by composite score descending across multiple signals")
        void resultIsSortedByScoreDescending() throws Exception {
            // best:   category(+30) + traffic>5M(+40) + enriched(+20) + format(+15) = 105
            // middle: category(+30) + traffic>1M(+30)                               =  60
            // worst:  mismatch(-10) + traffic<100K(+10)                             =   0
            // pad:    0 (no signals)
            // pool=4, maxItems=3 → pad dropped
            MediaItem best = baseline()
                .title("best")
                .category("Technology")
                .similarwebVisits(6_000_000L)
                .description("Detailed editorial description.")
                .formatType("Article")
                .build();

            MediaItem middle = baseline()
                .title("middle")
                .category("Technology")
                .similarwebVisits(2_000_000L)
                .build();

            MediaItem worst = baseline()
                .title("worst")
                .category("Sports")
                .similarwebVisits(50_000L)
                .build();

            MediaItem pad = dummy("pad");

            RecommendationRequestDTO request = baseRequest()
                .categories(List.of("Technology"))
                .formatPreference("Article")
                .build();

            List<MediaItem> result = invoke(List.of(worst, best, pad, middle), request, 3);

            assertThat(result).extracting(MediaItem::getTitle)
                .containsExactly("best", "middle", "worst");
        }

        @Test
        @DisplayName("After trim, the returned items are the highest-scoring ones")
        void trimmedListContainsHighestScoringItems() throws Exception {
            // high1/high2: traffic>5M → +40 each
            // low1/low2:   no traffic → 0
            // Trim to 2 → must keep only the high-traffic pair
            MediaItem low1  = baseline().title("low_1").similarwebVisits(null).build();
            MediaItem low2  = baseline().title("low_2").similarwebVisits(null).build();
            MediaItem high1 = baseline().title("high_1").similarwebVisits(5_000_001L).build();
            MediaItem high2 = baseline().title("high_2").similarwebVisits(5_000_002L).build();

            List<MediaItem> result = invoke(
                List.of(low1, high1, low2, high2), baseRequest().build(), 2
            );

            assertThat(result).extracting(MediaItem::getTitle)
                .containsExactlyInAnyOrder("high_1", "high_2");
        }
    }
}
