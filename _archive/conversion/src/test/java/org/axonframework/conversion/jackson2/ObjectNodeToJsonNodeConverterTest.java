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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ObjectNodeToJsonNodeConverter}.
 *
 * @author Steven van Beelen
 */
class ObjectNodeToJsonNodeConverterTest {

    private final ObjectNodeToJsonNodeConverter testSubject = new ObjectNodeToJsonNodeConverter();

    @Test
    void validateSourceAndTargetType() {
        assertEquals(ObjectNode.class, testSubject.expectedSourceType());
        assertEquals(JsonNode.class, testSubject.targetType());
    }

    @Test
    void convert() {
        ObjectNode expectedJsonNode = new ObjectNode(JsonNodeFactory.instance);

        JsonNode result = testSubject.convert(expectedJsonNode);

        assertEquals(expectedJsonNode, result);
    }

    @Test
    void convertIsNullSafe() {
        assertDoesNotThrow(() -> testSubject.convert(null));
        assertNull(testSubject.convert(null));
    }
}