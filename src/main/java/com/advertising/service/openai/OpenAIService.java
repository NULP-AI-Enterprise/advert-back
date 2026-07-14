package com.advertising.service.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OpenAIService {

    @Qualifier("openAIWebClient")
    private final WebClient webClient;

    private final ObjectMapper objectMapper;

    @Value("${openai.model.chat}")
    private String chatModel;

    @Value("${openai.model.embedding}")
    private String embeddingModel;

    @Value("${openai.max-tokens}")
    private int maxTokens;

    @Value("${openai.temperature}")
    private double temperature;

    // Retry on 429 (rate-limit) with exponential backoff: 2s → 4s → 8s (3 attempts).
    // Tier 1 OpenAI accounts hit 30K TPM limits; a short wait usually clears the window.
    private static final Retry RATE_LIMIT_RETRY = Retry.backoff(3, Duration.ofSeconds(2))
        .maxBackoff(Duration.ofSeconds(16))
        .filter(t -> t instanceof WebClientResponseException e
            && (e.getStatusCode().value() == 429 || e.getStatusCode().is5xxServerError()))
        .doBeforeRetry(signal -> log.warn("[OpenAI] {} — retry #{} after backoff",
            signal.failure() instanceof WebClientResponseException ex
                ? "HTTP " + ex.getStatusCode().value() : "transient error",
            signal.totalRetries() + 1));

    /**
     * Standard chat completion — returns the full assistant message.
     */
    public Mono<String> chatCompletion(List<Map<String, String>> messages) {
        ObjectNode body = buildChatRequestBody(messages, false);
        logRequest("chatCompletion", messages);
        long startMs = System.currentTimeMillis();

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .retryWhen(RATE_LIMIT_RETRY)
            .doOnNext(response -> logResponse("chatCompletion", response, startMs))
            .map(response -> response
                .path("choices").get(0)
                .path("message").path("content").asText()
            )
            .doOnError(e -> log.error("[OpenAI] chatCompletion failed after {}ms: {}",
                System.currentTimeMillis() - startMs, e.getMessage(), e));
    }

    /**
     * Streaming chat completion — emits delta chunks as they arrive.
     */
    public Flux<String> chatCompletionStream(List<Map<String, String>> messages) {
        ObjectNode body = buildChatRequestBody(messages, true);
        logRequest("chatCompletionStream", messages);
        long startMs = System.currentTimeMillis();

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToFlux(String.class)
            .filter(line -> line.startsWith("data: ") && !line.equals("data: [DONE]"))
            .map(line -> line.substring(6))
            .mapNotNull(json -> {
                try {
                    JsonNode node = objectMapper.readTree(json);
                    return node.path("choices").get(0)
                               .path("delta").path("content").asText(null);
                } catch (Exception e) {
                    log.warn("[OpenAI] chatCompletionStream failed to parse chunk: {}", json);
                    return null;
                }
            })
            .filter(chunk -> chunk != null && !chunk.isEmpty())
            .doOnSubscribe(s -> log.debug("[OpenAI] chatCompletionStream started model={} messages={}", chatModel, messages.size()))
            .doOnComplete(() -> log.info("[OpenAI] chatCompletionStream complete in {}ms", System.currentTimeMillis() - startMs))
            .doOnError(e -> log.error("[OpenAI] chatCompletionStream failed after {}ms: {}",
                System.currentTimeMillis() - startMs, e.getMessage(), e));
    }

    /**
     * JSON-mode completion — forces the model to return valid JSON.
     */
    public Mono<JsonNode> chatCompletionJson(List<Map<String, String>> messages) {
        ObjectNode body = buildChatRequestBody(messages, false);
        body.putObject("response_format").put("type", "json_object");
        logRequest("chatCompletionJson", messages);
        long startMs = System.currentTimeMillis();

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .retryWhen(RATE_LIMIT_RETRY)
            .doOnNext(response -> logResponse("chatCompletionJson", response, startMs))
            .map(response -> {
                String content = response
                    .path("choices").get(0)
                    .path("message").path("content").asText();
                try {
                    return objectMapper.readTree(content);
                } catch (Exception e) {
                    throw new RuntimeException("OpenAI returned invalid JSON: " + content, e);
                }
            });
    }

    /**
     * Structured-output completion — guarantees the response matches the provided JSON Schema.
     */
    public Mono<JsonNode> chatCompletionStructured(
            List<Map<String, String>> messages,
            String schemaName,
            JsonNode schema) {

        ObjectNode body = buildChatRequestBody(messages, false);
        ObjectNode responseFormat = body.putObject("response_format");
        responseFormat.put("type", "json_schema");
        ObjectNode jsonSchema = responseFormat.putObject("json_schema");
        jsonSchema.put("name", schemaName);
        jsonSchema.put("strict", true);
        jsonSchema.set("schema", schema);

        logRequest("chatCompletionStructured[" + schemaName + "]", messages);
        long startMs = System.currentTimeMillis();

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .retryWhen(RATE_LIMIT_RETRY)
            .doOnNext(response -> logResponse("chatCompletionStructured[" + schemaName + "]", response, startMs))
            .map(response -> {
                String content = response
                    .path("choices").get(0)
                    .path("message").path("content").asText();
                try {
                    return objectMapper.readTree(content);
                } catch (Exception e) {
                    throw new RuntimeException("OpenAI structured output returned invalid JSON: " + content, e);
                }
            })
            .doOnError(e -> log.error("[OpenAI] chatCompletionStructured[{}] failed after {}ms: {}",
                schemaName, System.currentTimeMillis() - startMs, e.getMessage()));
    }

    /**
     * Generates an embedding vector for the provided text.
     */
    public Mono<float[]> createEmbedding(String text) {
        log.info("[OpenAI] createEmbedding model={} text_chars={} preview='{}'",
            embeddingModel, text.length(),
            text.length() > 120 ? text.substring(0, 120) + "…" : text);
        long startMs = System.currentTimeMillis();

        ObjectNode body = objectMapper.createObjectNode();
        body.put("model", embeddingModel);
        body.put("input", text);

        return webClient.post()
            .uri("/embeddings")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(response -> {
                JsonNode embeddingArray = response.path("data").get(0).path("embedding");
                float[] vector = new float[embeddingArray.size()];
                for (int i = 0; i < embeddingArray.size(); i++) {
                    vector[i] = (float) embeddingArray.get(i).asDouble();
                }
                JsonNode usage = response.path("usage");
                log.info("[OpenAI] createEmbedding ← {}ms dims={} tokens={}",
                    System.currentTimeMillis() - startMs,
                    vector.length,
                    usage.path("total_tokens").asInt(0));
                return vector;
            })
            .doOnError(e -> log.error("[OpenAI] createEmbedding failed after {}ms: {}",
                System.currentTimeMillis() - startMs, e.getMessage(), e));
    }

    // ── Private helpers ─────────────────────────────────────────────────────────

    private void logRequest(String method, List<Map<String, String>> messages) {
        String systemContent = messages.stream()
            .filter(m -> "system".equals(m.get("role")))
            .findFirst().map(m -> m.get("content")).orElse("");
        String lastUserContent = messages.stream()
            .filter(m -> "user".equals(m.get("role")))
            .reduce((a, b) -> b)
            .map(m -> m.get("content")).orElse("");

        log.info("[OpenAI] → {} model={} messages={} system_chars={} user_preview='{}'",
            method, chatModel, messages.size(), systemContent.length(),
            lastUserContent.length() > 200 ? lastUserContent.substring(0, 200) + "…" : lastUserContent);

        log.debug("[OpenAI] {} full_message_list:", method);
        for (int i = 0; i < messages.size(); i++) {
            Map<String, String> msg = messages.get(i);
            String role    = msg.getOrDefault("role", "?");
            String content = msg.getOrDefault("content", "");
            log.debug("[OpenAI]   [{}] role={} chars={} content='{}'",
                i, role, content.length(),
                content.length() > 300 ? content.substring(0, 300) + "…" : content);
        }
    }

    private void logResponse(String method, JsonNode response, long startMs) {
        long ms = System.currentTimeMillis() - startMs;
        JsonNode usage       = response.path("usage");
        JsonNode choicesNode = response.path("choices");
        String finishReason  = choicesNode.size() > 0
            ? choicesNode.get(0).path("finish_reason").asText("?") : "?";
        int promptTokens     = usage.path("prompt_tokens").asInt(0);
        int completionTokens = usage.path("completion_tokens").asInt(0);
        int totalTokens      = usage.path("total_tokens").asInt(0);

        log.info("[OpenAI] ← {} {}ms finish={} tokens={}p+{}c={}total",
            method, ms, finishReason, promptTokens, completionTokens, totalTokens);

        if (choicesNode.size() > 0) {
            String content = choicesNode.get(0).path("message").path("content").asText("");
            if (!content.isBlank()) {
                log.debug("[OpenAI] {} response_content chars={} preview='{}'",
                    method, content.length(),
                    content.length() > 600 ? content.substring(0, 600) + "…" : content);
            }
        }
    }

    private ObjectNode buildChatRequestBody(List<Map<String, String>> messages, boolean stream) {
        ObjectNode body = objectMapper.createObjectNode();
        body.put("model", chatModel);
        body.put("max_tokens", maxTokens);
        body.put("temperature", temperature);
        body.put("stream", stream);

        ArrayNode messagesNode = body.putArray("messages");
        messages.forEach(msg -> {
            ObjectNode msgNode = messagesNode.addObject();
            msgNode.put("role", msg.get("role"));
            msgNode.put("content", msg.get("content"));
        });

        return body;
    }
}
