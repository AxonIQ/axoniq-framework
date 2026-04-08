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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdentifierValidatorTest {

    private final IdentifierValidator validator = IdentifierValidator.getInstance();

    @Test
    void boxedPrimitivesAreValidIdentifiers() {
        assertTrue(validator.isValidIdentifier(Long.class));
        assertTrue(validator.isValidIdentifier(Integer.class));
        assertTrue(validator.isValidIdentifier(Double.class));
        assertTrue(validator.isValidIdentifier(Short.class));
    }

    @Test
    void stringIsValidIdentifier() {
        assertTrue(validator.isValidIdentifier(CharSequence.class));
    }

    @Test
    void typeWithoutToStringIsNotAccepted() {
        assertFalse(validator.isValidIdentifier(CustomType.class));
    }

    @Test
    void typeWithOverriddenToString() {
        assertTrue(validator.isValidIdentifier(CustomType2.class));
    }

    private static class CustomType {
    }

    private static class CustomType2 {
        @Override
        public String toString() {
            return "ok";
        }
    }
}