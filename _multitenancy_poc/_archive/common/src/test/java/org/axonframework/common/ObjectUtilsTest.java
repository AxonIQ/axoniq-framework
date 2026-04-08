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

package org.axonframework.common;

import org.junit.jupiter.api.*;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ObjectUtils}.
 *
 * @author Steven van Beelen
 */
class ObjectUtilsTest {

    private static final String NULL_INSTANCE = null;
    private static final String INSTANCE = "instance";
    private static final String DEFAULT_VALUE = "default";

    @Test
    void getOrDefaultUsingValueProvider() {
        Function<String, String> valueProvider = o -> o;
        assertEquals(DEFAULT_VALUE, ObjectUtils.getOrDefault(NULL_INSTANCE, valueProvider, DEFAULT_VALUE));
        assertEquals(INSTANCE, ObjectUtils.getOrDefault(INSTANCE, valueProvider, DEFAULT_VALUE));
    }

    @Test
    void supplySameInstance() {
        Supplier<Object> testSubject = ObjectUtils.sameInstanceSupplier(Object::new);
        assertSame(testSubject.get(), testSubject.get());
    }
}