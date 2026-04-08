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

import java.util.Random;

import static org.axonframework.common.Assert.assertStrictPositive;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@code static} methods of the {@link Assert} utility.
 *
 * @author Allard Buijze
 */
class AssertTest {

    private static final int NUMBER_OF_RANDOM_NUMBERS = 256;

    @Test
    void stateAccept() {
        Assert.state(true, () -> "Hello");
    }

    @Test
    void stateFail() {
        assertThrows(IllegalStateException.class, () -> Assert.state(false, () -> "Hello"));
    }

    @Test
    void isTrueAccept() {
        Assert.isTrue(true, () -> "Hello");
    }

    @Test
    void isTrueFail() {
        assertThrows(IllegalArgumentException.class, () -> Assert.isTrue(false, () -> "Hello"));
    }

    @Test
    void nonEmpty() {
        assertEquals("some-text", Assert.nonEmpty("some-text", "Reacts fine on some text"));
        assertThrows(IllegalArgumentException.class, () -> Assert.nonEmpty(null, "Should fail on null"));
        assertThrows(IllegalArgumentException.class, () -> Assert.nonEmpty("", "Should fail on an empty string"));
    }

    @Test
    void assertStrictPositiveInteger() {
        // Fixed tests
        assertStrictPositive(1, "One is positive");
        assertThrows(IllegalArgumentException.class, () -> assertStrictPositive(0, "Zero is not strict positive"));
        assertThrows(IllegalArgumentException.class, () -> assertStrictPositive(-1, "Minus one is not positive"));

        // Random sample
        Random random = new Random();
        for (int i = 0; i < NUMBER_OF_RANDOM_NUMBERS; i++) {
            int value = random.nextInt();
            if (value > 0) {
                assertStrictPositive(value, "Value " + value + " is positive.");
            } else {
                assertThrows(IllegalArgumentException.class, () -> assertStrictPositive(value, "fail"));
            }
        }
    }

    @Test
    void assertStrictPositiveLong() {
        // Fixed tests
        assertStrictPositive(1L, "One is also positive");
        assertThrows(IllegalArgumentException.class, () -> assertStrictPositive(0L, "Zero is not strict positive"));
        assertThrows(IllegalArgumentException.class, () -> assertStrictPositive(-1L, "Minus one is not positive"));

        // Random sample
        Random random = new Random();
        for (int i = 0; i < NUMBER_OF_RANDOM_NUMBERS; i++) {
            long value = random.nextLong();
            if (value > 0L) {
                assertStrictPositive(value, "Value " + value + " is positive.");
            } else {
                assertThrows(IllegalArgumentException.class, () -> assertStrictPositive(value, "fail"));
            }
        }
    }
}
