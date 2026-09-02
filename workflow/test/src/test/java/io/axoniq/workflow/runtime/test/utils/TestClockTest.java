/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.test.utils;

import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TestClock}.
 *
 * @author Simon Zambrovski
 */
class TestClockTest {

    @Test
    void advanceByMovesCurrentInstant() {
        Instant initial = Instant.parse("2026-06-19T10:15:30Z");
        ZoneId zone = ZoneId.of("Europe/Berlin");
        TestClock clock = new TestClock(initial, zone);

        Instant advanced = clock.advanceBy(Duration.ofMinutes(5));

        assertThat(advanced).isEqualTo(Instant.parse("2026-06-19T10:20:30Z"));
        assertThat(clock.instant()).isEqualTo(advanced);
        assertThat(clock.getZone()).isEqualTo(zone);
    }

    @Test
    void withZoneKeepsInstantAndUsesRequestedZone() {
        Instant initial = Instant.parse("2026-06-19T10:15:30Z");
        TestClock clock = new TestClock(initial, ZoneId.of("Europe/Berlin"));

        Clock zonedClock = clock.withZone(ZoneId.of("UTC"));

        assertThat(zonedClock.instant()).isEqualTo(initial);
        assertThat(zonedClock.getZone()).isEqualTo(ZoneId.of("UTC"));
        assertThat(clock.getZone()).isEqualTo(ZoneId.of("Europe/Berlin"));
    }
}
