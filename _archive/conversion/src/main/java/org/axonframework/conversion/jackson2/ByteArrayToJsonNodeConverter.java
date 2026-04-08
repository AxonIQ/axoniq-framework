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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;
import org.axonframework.conversion.ContentTypeConverter;
import org.axonframework.conversion.ConversionException;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link ContentTypeConverter} implementation for Jackson 2 that converts {@code byte[]} containing UTF8 encoded JSON
 * string to a {@link JsonNode}.
 *
 * @author Allard Buijze
 * @since 2.2.0
 */
public class ByteArrayToJsonNodeConverter implements ContentTypeConverter<byte[], JsonNode> {

    private final ObjectMapper objectMapper;

    /**
     * Initialize the Converter, using given {@code objectMapper} to parse the binary contents
     *
     * @param objectMapper the Jackson ObjectMapper to parse the byte array with
     */
    public ByteArrayToJsonNodeConverter(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "The ObjectMapper may not be null.");
    }

    @Override
    public Class<byte[]> expectedSourceType() {
        return byte[].class;
    }

    @Override
    public Class<JsonNode> targetType() {
        return JsonNode.class;
    }

    @Override
    @Nullable
    public JsonNode convert(byte @Nullable[] input) {
        if (input == null) {
            return null;
        }

        try {
            return objectMapper.readTree(input);
        } catch (IOException e) {
            throw new ConversionException("An error occurred while converting a JsonNode to byte[].", e);
        }
    }
}
