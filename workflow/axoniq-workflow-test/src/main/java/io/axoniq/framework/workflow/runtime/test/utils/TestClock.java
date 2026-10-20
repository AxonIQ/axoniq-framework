/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.test.utils;

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
 * @since 5.4.0
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
