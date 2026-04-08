/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.conversion.jackson2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;
import org.axonframework.conversion.ContentTypeConverter;
import org.axonframework.conversion.ConversionException;


import java.util.Objects;

/**
 * A {@link ContentTypeConverter} implementation for Jackson 2 that converts a {@link JsonNode} object into a
 * {@code byte[]}.
 * <p>
 * The {@code byte[]} will contain the UTF8 encoded JSON string.
 *
 * @author Allard Buijze
 * @since 2.2.0
 */
public class JsonNodeToByteArrayConverter implements ContentTypeConverter<JsonNode, byte[]> {

    private final ObjectMapper objectMapper;

    /**
     * Initialize the {@code JsonNodeToByteArrayConverter} using the given {@code objectMapper} to convert the
     * {@link JsonNode} into {@code byte[]}.
     *
     * @param objectMapper The object mapper to serialize the {@link JsonNode} into {@code byte[].
     */
    public JsonNodeToByteArrayConverter(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "The ObjectMapper may not be null.");
    }

    @Override
    public Class<JsonNode> expectedSourceType() {
        return JsonNode.class;
    }

    @Override
    public Class<byte[]> targetType() {
        return byte[].class;
    }

    @Override
    public byte @Nullable [] convert(@Nullable JsonNode input) {
        try {
            return objectMapper.writeValueAsBytes(input);
        } catch (JsonProcessingException e) {
            throw new ConversionException("An error occurred while converting a JsonNode to byte[]", e);
        }
    }
}
