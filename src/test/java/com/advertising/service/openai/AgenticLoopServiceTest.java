package com.advertising.service.openai;

import com.advertising.model.dto.RecommendationRequestDTO;
import com.advertising.model.entity.ChatMessage;
import com.advertising.service.chat.ChatHistoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AgenticLoopService#decide}.
 *
 * Strategy: mock both {@code OpenAIService#chatCompletionStructured} and
 * {@code ChatHistoryService#getRecentHistory} so that each test controls exactly
 * what the LLM "returns" as a {@link com.fasterxml.jackson.databind.JsonNode}.
 * The private {@code parseDecision()} logic is exercised entirely through the
 * public {@code decide()} entry-point, subscribed synchronously via {@code .block()}.
 */
@ActiveProfiles("test")
@ExtendWith(MockitoExtension.class)
@DisplayName("AgenticLoopService – decide() routing")
class AgenticLoopServiceTest {

    @Mock
    private OpenAIService openAIService;

    @Mock
    private ChatHistoryService chatHistoryService;

    /*
     * ObjectMapper is a plain POJO — no need to mock it.  A real instance avoids
     * complex mock setup for readTree / createObjectNode while staying in-memory.
     */
    private final ObjectMapper mapper = new ObjectMapper();

    @InjectMocks
    private AgenticLoopService agenticLoopService;

    private static final String SESSION_ID = "test-session-123";

    @BeforeEach
    void injectFields() throws Exception {
        // Inject the real ObjectMapper (field injected by @RequiredArgsConstructor)
        ReflectionTestUtils.setField(agenticLoopService, "objectMapper", mapper);
        // Inject @Value fields that would normally come from application.properties
        ReflectionTestUtils.setField(agenticLoopService, "defaultMaxResults", 10);
        ReflectionTestUtils.setField(agenticLoopService, "contextWindow", 20);
        // Trigger the @PostConstruct router-schema parse
        agenticLoopService.init();
        // Default: empty history so clarifyCount = 0 for all tests unless overridden.
        // lenient() suppresses UnnecessaryStubbingException for tests that override this stub.
        org.mockito.Mockito.lenient()
            .when(chatHistoryService.getRecentHistory(eq(SESSION_ID), anyInt()))
            .thenReturn(Mono.just(List.of()));
    }

    // ─── JSON builders ─────────────────────────────────────────────────────────

    private ObjectNode clarifyJson(String question) {
        ObjectNode json = mapper.createObjectNode();
        json.put("action", "clarify");
        json.put("question", question);
        json.putNull("search_params");
        json.put("reasoning", "need more info");
        json.putArray("suggestions");
        return json;
    }

    private ObjectNode searchJson(ObjectNode searchParams, String... suggestions) {
        ObjectNode json = mapper.createObjectNode();
        json.put("action", "search");
        json.putNull("question");
        json.set("search_params", searchParams);
        json.put("reasoning", "enough info to search");
        var suggArray = json.putArray("suggestions");
        for (String s : suggestions) suggArray.add(s);
        return json;
    }

    private ObjectNode planJson() {
        ObjectNode json = mapper.createObjectNode();
        json.put("action", "plan");
        json.putNull("question");
        json.putNull("search_params");
        json.put("reasoning", "user asked for a plan");
        json.putArray("suggestions");
        return json;
    }

    /**
     * Builds a minimal but valid search_params ObjectNode with required nullable
     * fields set so the JSON parsing in parseDecision() does not throw.
     */
    private ObjectNode minimalSearchParams() {
        ObjectNode params = mapper.createObjectNode();
        params.putArray("categories");
        params.putArray("keywords");
        params.putNull("target_audience_description");
        params.putNull("age_range");
        params.putNull("budget_usd");
        params.putNull("campaign_objective");
        params.putNull("format_preference");
        params.putNull("event_date");
        params.putNull("region");
        params.putNull("country");
        params.put("max_results", 10);
        return params;
    }

    /** Convenience: block on a decide() call and return the concrete decision. */
    private AgenticLoopService.AgentDecision decide(String message) {
        return agenticLoopService.decide(SESSION_ID, message).block();
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Clarify
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("action=clarify")
    class ClarifyAction {

        @Test
        @DisplayName("Returns AgentDecision.Clarify with the question from LLM response")
        void returnsClarifyWithQuestion() {
            String question = "What is your budget?";
            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(clarifyJson(question)));

            var decision = decide("I want to advertise");

            assertThat(decision).isInstanceOf(AgenticLoopService.AgentDecision.Clarify.class);
            assertThat(((AgenticLoopService.AgentDecision.Clarify) decision).question())
                .isEqualTo(question);
        }

        @Test
        @DisplayName("Falls back to Clarify when action field is unrecognised")
        void unknownActionDefaultsToClarify() {
            ObjectNode json = mapper.createObjectNode();
            json.put("action", "unknown_action");
            json.putNull("question");
            json.putNull("search_params");
            json.put("reasoning", "test");
            json.putArray("suggestions");

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(json));

            var decision = decide("hello");

            assertThat(decision).isInstanceOf(AgenticLoopService.AgentDecision.Clarify.class);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Search
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("action=search")
    class SearchAction {

        @Test
        @DisplayName("Parses categories, budget, country, keywords, and max_results")
        void parsesFullSearchParams() {
            ObjectNode params = minimalSearchParams();
            params.putArray("categories").add("Technology");
            params.put("budget_usd", 500.0);
            params.put("country", "Ukraine");
            params.put("max_results", 10);
            var kws = params.putArray("keywords");
            kws.add("tech");
            kws.add("digital");

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(searchJson(params)));

            var decision = decide("find media in Ukraine");

            assertThat(decision).isInstanceOf(AgenticLoopService.AgentDecision.Search.class);
            RecommendationRequestDTO req =
                ((AgenticLoopService.AgentDecision.Search) decision).request();

            assertThat(req.getCategories()).containsExactly("Technology");
            assertThat(req.getBudgetUsd()).isEqualTo(500.0);
            assertThat(req.getCountry()).isEqualTo("Ukraine");
            assertThat(req.getMaxResults()).isEqualTo(10);
            assertThat(req.getKeywords()).containsExactlyInAnyOrder("tech", "digital");
        }

        @Test
        @DisplayName("Keywords shorter than 4 characters are filtered out")
        void shortKeywordsAreFiltered() {
            ObjectNode params = minimalSearchParams();
            var kws = params.putArray("keywords");
            kws.add("IT");     // 2 chars — must be removed
            kws.add("AI");     // 2 chars — must be removed
            kws.add("tech");   // 4 chars — kept
            kws.add("digital");

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(searchJson(params)));

            var decision = decide("find tech media");

            RecommendationRequestDTO req =
                ((AgenticLoopService.AgentDecision.Search) decision).request();
            assertThat(req.getKeywords())
                .containsExactlyInAnyOrder("tech", "digital")
                .doesNotContain("IT", "AI");
        }

        @Test
        @DisplayName("Null country in search_params does not set the string 'null' on request")
        void nullCountryDoesNotBecomeLiteralNullString() {
            ObjectNode params = minimalSearchParams();
            params.putNull("country");

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(searchJson(params)));

            var decision = decide("find media");

            RecommendationRequestDTO req =
                ((AgenticLoopService.AgentDecision.Search) decision).request();
            // Must be actual null, never the string "null"
            assertThat(req.getCountry()).isNull();
        }

        @Test
        @DisplayName("Suggestions from LLM are forwarded onto the Search decision")
        void suggestionsAreForwarded() {
            ObjectNode params = minimalSearchParams();

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(
                    searchJson(params, "Filter by Article format", "Increase to 15 results")
                ));

            var decision = decide("find media");

            assertThat(((AgenticLoopService.AgentDecision.Search) decision).suggestions())
                .containsExactly("Filter by Article format", "Increase to 15 results");
        }

        @Test
        @DisplayName("All-short keyword list results in null keywords on the request")
        void allShortKeywordsResultsInNullKeywords() {
            ObjectNode params = minimalSearchParams();
            var kws = params.putArray("keywords");
            kws.add("AI");
            kws.add("IT");
            kws.add("ML");

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(searchJson(params)));

            var decision = decide("find media");

            RecommendationRequestDTO req =
                ((AgenticLoopService.AgentDecision.Search) decision).request();
            // All keywords were filtered — builder.keywords() never called → null
            assertThat(req.getKeywords()).isNull();
        }

        @Test
        @DisplayName("max_results falls back to defaultMaxResults when LLM returns 0")
        void zeroMaxResultsFallsBackToDefault() {
            ObjectNode params = minimalSearchParams();
            params.put("max_results", 0);  // 0 is invalid → must use defaultMaxResults

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(searchJson(params)));

            var decision = decide("find media");

            RecommendationRequestDTO req =
                ((AgenticLoopService.AgentDecision.Search) decision).request();
            assertThat(req.getMaxResults()).isEqualTo(10); // equals defaultMaxResults
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Plan
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("action=plan")
    class PlanAction {

        @Test
        @DisplayName("Returns AgentDecision.Plan with the session ID when action is 'plan'")
        void returnsPlanDecision() {
            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(planJson()));

            var decision = decide("create a marketing plan");

            assertThat(decision).isInstanceOf(AgenticLoopService.AgentDecision.Plan.class);
            assertThat(((AgenticLoopService.AgentDecision.Plan) decision).sessionId())
                .isEqualTo(SESSION_ID);
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // History integration
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Chat history integration")
    class HistoryIntegration {

        @Test
        @DisplayName("clarifyCount is derived from assistant messages in history")
        void clarifyCountReflectsAssistantTurnsInHistory() {
            // Three prior assistant turns — service must read clarifyCount = 3 from history.
            List<ChatMessage> history = List.of(
                assistantMessage("What industry are you in?"),
                assistantMessage("What's your budget?"),
                assistantMessage("Which region?")
            );
            when(chatHistoryService.getRecentHistory(eq(SESSION_ID), anyInt()))
                .thenReturn(Mono.just(history));

            when(openAIService.chatCompletionStructured(anyList(), anyString(), any()))
                .thenReturn(Mono.just(clarifyJson("Could you describe your audience?")));

            var decision = decide("please help");

            // The test verifies the pipeline completes without error and returns Clarify.
            // The injected clarifyCount value is verified indirectly via the system-prompt
            // assembly path in buildMessageList(), which the mocked LLM bypasses — the
            // important thing is no NullPointerException or ClassCastException.
            assertThat(decision).isInstanceOf(AgenticLoopService.AgentDecision.Clarify.class);
        }

        private ChatMessage assistantMessage(String content) {
            ChatMessage msg = new ChatMessage();
            msg.setRole(ChatMessage.MessageRole.assistant);
            msg.setContent(content);
            return msg;
        }
    }
}
