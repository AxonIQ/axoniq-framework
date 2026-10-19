/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

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
