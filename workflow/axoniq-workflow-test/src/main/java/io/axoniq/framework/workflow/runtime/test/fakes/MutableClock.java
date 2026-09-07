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
package io.axoniq.framework.workflow.runtime.test.fakes;


import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Controllable {@link Clock} fake whose instant only changes when the test advances or sets it.
 * <p>
 * The engine already resolves time through an injected {@link Clock}; registering this clock lets a deterministic
 * simulator drive all of the engine's time math (step timestamps, timeout/backoff computations) from a single,
 * explicitly advanced source. It never reads the wall clock.
 * <p>
 * Register this in place of the default clock via the configurer:
 * {@code componentRegistry(cr -> cr.registerComponent(Clock.class, cfg -> clock))}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class MutableClock extends Clock {

    private final AtomicReference<Instant> instant;
    private final ZoneId zone;

    /**
     * Creates a clock fixed at the Unix epoch in UTC.
     */
    public MutableClock() {
        this(Instant.EPOCH, ZoneOffset.UTC);
    }

    /**
     * Creates a clock fixed at the given instant in UTC.
     *
     * @param instant initial instant.
     */
    public MutableClock(Instant instant) {
        this(instant, ZoneOffset.UTC);
    }

    /**
     * Creates a clock fixed at the given instant in the given zone.
     *
     * @param instant initial instant.
     * @param zone    clock zone.
     */
    public MutableClock(Instant instant, ZoneId zone) {
        this.instant = new AtomicReference<>(instant);
        this.zone = zone;
    }

        @Override
    public ZoneId getZone() {
        return zone;
    }

        @Override
    public Clock withZone(ZoneId newZone) {
        return new MutableClock(instant.get(), newZone);
    }

        @Override
    public Instant instant() {
        return instant.get();
    }

    /**
     * Sets the clock to the given instant.
     *
     * @param newInstant instant to set.
     */
    public void setInstant(Instant newInstant) {
        instant.set(newInstant);
    }

    /**
     * Advances the clock by the given duration.
     *
     * @param delta amount to advance.
     */
    public void advanceBy(Duration delta) {
        instant.updateAndGet(current -> current.plus(delta));
    }
}
