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

package org.axonframework.extension.spring.data;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Custom Jackson deserializer for the Spring Data {@link Page} interface.
 * <p>
 * This deserializer converts JSON representations of paginated data into {@link PageImpl} instances. It extracts the
 * {@code content} array, {@code number} (page number), {@code size} (page size), and {@code totalElements} from the
 * JSON structure.
 * <p>
 * The deserializer handles missing fields gracefully by applying sensible defaults:
 * <ul>
 *     <li>{@code number} defaults to 0</li>
 *     <li>{@code size} defaults to the content size (minimum 1)</li>
 *     <li>{@code totalElements} defaults to the content size</li>
 * </ul>
 *
 * @author Theo Emanuelsson
 * @since 5.1.0
 */
public class JacksonPageDeserializer extends ValueDeserializer<Page<?>> {

    @Override
    public Page<?> deserialize(JsonParser p, DeserializationContext ctxt) {
        JsonNode node = p.objectReadContext().readTree(p);

        List<Object> content = new ArrayList<>();
        JsonNode contentNode = node.get("content");
        if (contentNode != null && contentNode.isArray()) {
            contentNode.forEach(content::add);
        }

        int page = node.has("number") ? node.get("number").asInt() : 0;
        int size = node.has("size") ? node.get("size").asInt() : Math.max(content.size(), 1);
        long totalElements = node.has("totalElements") ? node.get("totalElements").asLong() : content.size();

        return new PageImpl<>(content, PageRequest.of(page, size), totalElements);
    }
}
