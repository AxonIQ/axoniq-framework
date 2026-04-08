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

package org.axonframework.conversion.jackson;

import org.axonframework.conversion.ContentTypeConverter;
import org.axonframework.conversion.ConversionException;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeType;
import tools.jackson.databind.node.ObjectNode;

/**
 * A {@link ContentTypeConverter} implementation that converts a {@link JsonNode} into an {@link ObjectNode}.
 * <p>
 * Intended to simplify JSON-typed event upcasters, which generally deal with an {@code ObjectNode} as the event.
 * <p>
 * Will succeed if the {@code JsonNode} has a node type of {@link JsonNodeType#OBJECT}.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class JsonNodeToObjectNodeConverter implements ContentTypeConverter<JsonNode, ObjectNode> {

    @Override
    public Class<JsonNode> expectedSourceType() {
        return JsonNode.class;
    }

    @Override
    public Class<ObjectNode> targetType() {
        return ObjectNode.class;
    }

    @Override
    @Nullable
    public ObjectNode convert(@Nullable JsonNode input) {
        if (input == null) {
            return null;
        }

        JsonNodeType originalNodeType = input.getNodeType();
        if (JsonNodeType.OBJECT.equals(originalNodeType)) {
            return ((ObjectNode) input);
        } else {
            throw new ConversionException(
                    "Cannot convert from JsonNode to ObjectNode because the node type is [" + originalNodeType + "]."
            );
        }
    }
}
