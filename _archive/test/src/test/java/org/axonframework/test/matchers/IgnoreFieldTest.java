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

package org.axonframework.test.matchers;

import org.axonframework.test.FixtureExecutionException;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Allard Buijze
 */
class IgnoreFieldTest {

    @SuppressWarnings("unused") private String field;
    @SuppressWarnings("unused") private String ignoredField;

    @Test
    void acceptOtherFields_ClassStringConstructor() throws Exception {
        IgnoreField testSubject = new IgnoreField(IgnoreFieldTest.class, "ignoredField");
        assertTrue(testSubject.accept(IgnoreFieldTest.class.getDeclaredField("field")));
        assertFalse(testSubject.accept(IgnoreFieldTest.class.getDeclaredField("ignoredField")));
    }

    @Test
    void acceptOtherFields_FieldConstructor() throws Exception {
        IgnoreField testSubject = new IgnoreField(IgnoreFieldTest.class.getDeclaredField("ignoredField"));
        assertTrue(testSubject.accept(IgnoreFieldTest.class.getDeclaredField("field")));
        assertFalse(testSubject.accept(IgnoreFieldTest.class.getDeclaredField("ignoredField")));
    }

    @Test
    void rejectNonExistentField() {
        assertThrows(FixtureExecutionException.class, () -> new IgnoreField(IgnoreFieldTest.class, "nonExistent"));
    }
}
