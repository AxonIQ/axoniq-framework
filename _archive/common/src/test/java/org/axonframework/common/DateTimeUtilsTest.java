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

import java.time.Instant;
import java.time.temporal.ChronoField;

import static org.axonframework.common.DateTimeUtils.formatInstant;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link DateTimeUtils}.
 *
 * @author Allard Buijze
 */
class DateTimeUtilsTest {

    @Test
    void formattedDateAlwaysContainsMillis() {
        Instant now = Instant.now();
        Instant nowAtZeroMillis = now.minusNanos(now.get(ChronoField.NANO_OF_SECOND));

        String formatted = formatInstant(nowAtZeroMillis);
        assertTrue(formatted.matches(".*\\.0{3,}Z"), "Time doesn't seem to contain explicit millis: " + formatted);

        assertEquals(nowAtZeroMillis, DateTimeUtils.parseInstant(formatted));
    }

    @Test
    void formatInstantHasFixedPrecisionAtThree() {
        String expectedDateTimeString = "2021-03-22T15:41:02.101Z";

        Instant testInstantWithTrailingZeroes = Instant.parse("2021-03-22T15:41:02.101900Z");
        Instant testInstantWithTrailingNonZeroes = Instant.parse("2021-03-22T15:41:02.101911Z");
        Instant testInstantWithoutTrailingZeroes = Instant.parse("2021-03-22T15:41:02.1019Z");
        Instant testInstantWithoutMillis = Instant.parse("2021-03-22T15:41:02Z");

        assertEquals(expectedDateTimeString, formatInstant(testInstantWithTrailingZeroes));
        assertEquals(expectedDateTimeString, formatInstant(testInstantWithTrailingNonZeroes));
        assertEquals(expectedDateTimeString, formatInstant(testInstantWithoutTrailingZeroes));
        assertEquals("2021-03-22T15:41:02.000Z", formatInstant(testInstantWithoutMillis));
    }
}
