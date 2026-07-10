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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

    /**
     * Standard chat completion — returns the full assistant message.
     */
    public Mono<String> chatCompletion(List<Map<String, String>> messages) {
        ObjectNode body = buildChatRequestBody(messages, false);

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(response -> response
                .path("choices").get(0)
                .path("message").path("content").asText()
            )
            .doOnError(e -> log.error("OpenAI chat completion failed", e));
    }

    /**
     * Streaming chat completion — emits delta chunks as they arrive.
     */
    public Flux<String> chatCompletionStream(List<Map<String, String>> messages) {
        ObjectNode body = buildChatRequestBody(messages, true);

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
                    log.warn("Failed to parse stream chunk: {}", json);
                    return null;
                }
            })
            .filter(chunk -> chunk != null && !chunk.isEmpty());
    }

    /**
     * JSON-mode completion — forces the model to return valid JSON.
     * Use for structured outputs (search params, plan skeleton).
     */
    public Mono<JsonNode> chatCompletionJson(List<Map<String, String>> messages) {
        ObjectNode body = buildChatRequestBody(messages, false);
        body.putObject("response_format").put("type", "json_object");

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
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
     * Uses OpenAI's json_schema response_format with strict=true.
     * Eliminates prompt-based JSON description and prevents hallucinated field names.
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

        return webClient.post()
            .uri("/chat/completions")
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode.class)
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
            .doOnError(e -> log.error("OpenAI structured completion failed: {}", e.getMessage()));
    }

    /**
     * Generates an embedding vector for the provided text.
     * Used for pgvector similarity search.
     */
    public Mono<float[]> createEmbedding(String text) {
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
                return vector;
            });
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
