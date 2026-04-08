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

package org.axonframework.conversion.converter;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link InputStreamToByteArrayConverter}.
 *
 * @author Allard Buijze
 */
class InputStreamToByteArrayConverterTest {

    private InputStreamToByteArrayConverter testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new InputStreamToByteArrayConverter();
    }

    @Test
    void validateSourceAndTargetType() {
        assertEquals(InputStream.class, testSubject.expectedSourceType());
        assertEquals(byte[].class, testSubject.targetType());
    }

    @Test
    void convert() {
        byte[] bytes = "Hello, world!".getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        byte[] actual = testSubject.convert(inputStream);

        assertArrayEquals(bytes, actual);
    }

    @Test
    void convertIsNullSafe() {
        assertDoesNotThrow(() -> testSubject.convert(null));
        assertNull(testSubject.convert(null));
    }
}
