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

import org.axonframework.conversion.ConversionException;
import org.junit.jupiter.api.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link JsonNodeToObjectNodeConverter}.
 *
 * @author Steven van Beelen
 */
class JsonNodeToObjectNodeConverterTest {

    private final JsonNodeToObjectNodeConverter testSubject = new JsonNodeToObjectNodeConverter();

    @Test
    void validateSourceAndTargetType() {
        assertEquals(JsonNode.class, testSubject.expectedSourceType());
        assertEquals(ObjectNode.class, testSubject.targetType());
    }

    @Test
    void convert() {
        JsonNode expectedJsonNode = new ObjectNode(JsonNodeFactory.instance);

        ObjectNode result = testSubject.convert(expectedJsonNode);

        assertEquals(expectedJsonNode, result);
    }

    @Test
    void convertThrowsException() {
        JsonNode testJsonNode = new StringNode("some-text");

        assertThrows(ConversionException.class, () -> testSubject.convert(testJsonNode));
    }

    @Test
    void convertIsNullSafe() {
        assertDoesNotThrow(() -> testSubject.convert(null));
        assertNull(testSubject.convert(null));
    }
}