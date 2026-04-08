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

import org.axonframework.common.io.IOUtils;
import org.junit.jupiter.api.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ByteArrayToJsonNodeConverter}.
 *
 * @author Allard Buijze
 */
class ByteArrayToJsonNodeConverterTest {

    private ByteArrayToJsonNodeConverter testSubject;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = JsonMapper.builder().build();
        testSubject = new ByteArrayToJsonNodeConverter(objectMapper);
    }

    @Test
    void throwsNullPointerExceptionWhenConstructingWithNullObjectMapper() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new ByteArrayToJsonNodeConverter(null));
    }

    @Test
    void validateSourceAndTargetType() {
        assertEquals(byte[].class, testSubject.expectedSourceType());
        assertEquals(JsonNode.class, testSubject.targetType());
    }

    @Test
    void convert() throws Exception {
        final String content = "{\"someKey\":\"someValue\",\"someOther\":true}";
        JsonNode expected = objectMapper.readTree(content);
        assertEquals(expected, testSubject.convert(content.getBytes(IOUtils.UTF8)));
    }

    @Test
    void convertIsNullSafe() {
        assertDoesNotThrow(() -> testSubject.convert(null));
        assertNull(testSubject.convert(null));
    }
}
