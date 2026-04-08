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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jspecify.annotations.Nullable;
import org.axonframework.conversion.ContentTypeConverter;

/**
 * A {@link ContentTypeConverter} implementation for Jackson 2 that converts an {@link ObjectNode} object into a
 * {@link JsonNode}. Intended to simplify JSON-typed event upcasters, which generally deal with an {@code ObjectNode} as
 * the event.
 * <p>
 * Will succeed converting at all times as an {@code ObjectNode} is a {@code JsonNode} by definition.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class ObjectNodeToJsonNodeConverter implements ContentTypeConverter<ObjectNode, JsonNode> {

    @Override
    public Class<ObjectNode> expectedSourceType() {
        return ObjectNode.class;
    }

    @Override
    public Class<JsonNode> targetType() {
        return JsonNode.class;
    }

    @Override
    public @Nullable JsonNode convert(@Nullable ObjectNode input) {
        return input;
    }
}
