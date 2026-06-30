package com.advertising.model.dto;

import com.advertising.model.entity.ChatMessage.MessageRole;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatMessageDTO {

    private UUID id;

    @NotBlank
    private String sessionId;

    @NotNull
    private MessageRole role;

    @NotBlank
    private String content;

    private Map<String, Object> metadata;

    private OffsetDateTime createdAt;
}
