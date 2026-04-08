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

package org.axonframework.extension.metrics.micrometer.reservoir;

import io.micrometer.core.instrument.Clock;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Adapted from com.codahale.metrics.SlidingTimeWindowReservoirTest from io.dropwizard.metrics:metrics-core:3.1.2
 */
class SlidingTimeWindowReservoirTest {
    private final Clock clock = mock(Clock.class);
    private final SlidingTimeWindowReservoir reservoir = new SlidingTimeWindowReservoir(10, TimeUnit.NANOSECONDS, clock);

    @Test
    void storesMeasurementsWithDuplicateTicks() {
        when(clock.monotonicTime()).thenReturn(20L);

        reservoir.update(1L);
        reservoir.update(2L);

        assertEquals(Arrays.asList(1L, 2L), reservoir.getMeasurements());
    }

    @Test
    void boundsMeasurementsToATimeWindow() {
        when(clock.monotonicTime()).thenReturn(0L);
        reservoir.update(1L);

        when(clock.monotonicTime()).thenReturn(5L);
        reservoir.update(2L);

        when(clock.monotonicTime()).thenReturn(10L);
        reservoir.update(3L);

        when(clock.monotonicTime()).thenReturn(15L);
        reservoir.update(4L);

        when(clock.monotonicTime()).thenReturn(20L);
        reservoir.update(5L);

        assertEquals(Arrays.asList(4L, 5L), reservoir.getMeasurements());
    }
}
