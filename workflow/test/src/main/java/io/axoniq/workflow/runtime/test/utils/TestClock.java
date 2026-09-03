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

import org.axonframework.common.annotation.Internal;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable clock for fixture-controlled workflow time.
 *
 * @author Simon Zambrovski
 * @since 0.2.0
 */
@Internal
public class TestClock extends Clock {

    private final AtomicReference<Instant> instant;
    private final ZoneId zone;

    /**
     * Creates a mutable clock.
     */
    public TestClock() {
        this(Instant.now(), ZoneId.systemDefault());
    }

    /**
     * Creates a mutable clock.
     *
     * @param instant initial instant
     * @param zone    time zone
     */
    public TestClock(Instant instant, ZoneId zone) {
        this.instant = new AtomicReference<>(Objects.requireNonNull(instant, "Instant must not be null"));
        this.zone = Objects.requireNonNull(zone, "Zone must not be null");
    }

    /**
     * Returns the time zone used by this clock.
     *
     * @return clock time zone
     */
    @Override
    public ZoneId getZone() {
        return zone;
    }

    /**
     * Creates a mutable clock with the same instant and a different time zone.
     *
     * @param zone time zone for the returned clock
     * @return mutable clock using the requested zone
     */
    @Override
    public Clock withZone(ZoneId zone) {
        return new TestClock(instant(), zone);
    }

    /**
     * Returns the current fixture-controlled instant.
     *
     * @return current instant
     */
    @Override
    public Instant instant() {
        return instant.get();
    }

    /**
     * Advance the clock.
     *
     * @param duration duration to advance
     * @return new instant
     */
    public Instant advanceBy(Duration duration) {
        return instant.updateAndGet(current -> current.plus(duration));
    }
}
