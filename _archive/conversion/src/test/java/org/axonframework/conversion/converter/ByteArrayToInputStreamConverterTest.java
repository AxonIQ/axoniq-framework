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

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ByteArrayToInputStreamConverter}.
 *
 * @author Steven van Beelen
 */
class ByteArrayToInputStreamConverterTest {

    private ByteArrayToInputStreamConverter testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new ByteArrayToInputStreamConverter();
    }

    @Test
    void validateSourceAndTargetType() {
        assertEquals(byte[].class, testSubject.expectedSourceType());
        assertEquals(InputStream.class, testSubject.targetType());
    }

    @Test
    void convert() throws IOException {
        byte[] testObject = "Hello, world!".getBytes();

        InputStream result = testSubject.convert(testObject);

        assertNotNull(result);
        assertArrayEquals(testObject, result.readAllBytes());
    }

    @Test
    void convertIsNullSafe() {
        //noinspection resource
        assertDoesNotThrow(() -> testSubject.convert(null));
        assertNull(testSubject.convert(null));
    }
}