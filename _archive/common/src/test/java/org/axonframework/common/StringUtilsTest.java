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

import static org.axonframework.common.StringUtils.lowerCaseFirstCharacterOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link StringUtils}.
 *
 * @author Steven van Beelen
 */
class StringUtilsTest {

    @Test
    void nonEmptyOrNullReturnsFalseForEmptyString() {
        assertFalse(StringUtils.nonEmptyOrNull(""));
    }

    @Test
    void nonEmptyOrNullReturnsFalseForNullString() {
        assertFalse(StringUtils.nonEmptyOrNull(null));
    }

    @Test
    void nonEmptyOrNullReturnsTrueForNonEmptyString() {
        assertTrue(StringUtils.nonEmptyOrNull("some-string"));
    }

    @Test
    void emptyOrNullReturnsTrueForEmptyStrings() {
        assertTrue(StringUtils.emptyOrNull(""));
    }

    @Test
    void emptyOrNullReturnsTrueForNullString() {
        assertTrue(StringUtils.emptyOrNull(null));
    }

    @Test
    void emptyOrNullReturnsFalseForNonEmptyString() {
        assertFalse(StringUtils.emptyOrNull("some-string"));
    }

    @Test
    void lowerCaseFirstCharacterOfAdjustsFirstCharacterToLowerCase() {
        String fullUppercase = "FOO";
        String lowerCasedOutputOfFullUppercase = "fOO";
        assertEquals(lowerCasedOutputOfFullUppercase, lowerCaseFirstCharacterOf(fullUppercase));
        assertEquals(lowerCasedOutputOfFullUppercase, lowerCaseFirstCharacterOf(lowerCasedOutputOfFullUppercase));

        String partialUppercase = "FOo";
        String partialLowercase = "fOo";
        assertEquals(partialLowercase, lowerCaseFirstCharacterOf(partialUppercase));
        assertEquals(partialLowercase, lowerCaseFirstCharacterOf(partialLowercase));
    }

    @Test
    void capitalizeReturnsStringWithFirstCharacterUppercase() {
        String allLowercase = "foo";
        String capitalizedOutputOfAllLowercase = "Foo";
        assertEquals(capitalizedOutputOfAllLowercase, StringUtils.capitalize(allLowercase));
        assertEquals(capitalizedOutputOfAllLowercase, StringUtils.capitalize(capitalizedOutputOfAllLowercase));

        String partialUppercase = "fOo";
        String partialUppercaseOutput = "FOo";
        assertEquals(partialUppercaseOutput, StringUtils.capitalize(partialUppercase));
        assertEquals(partialUppercaseOutput, StringUtils.capitalize(partialUppercaseOutput));
    }
}