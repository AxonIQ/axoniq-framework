/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.function.Supplier;

/**
 * Fake implementation of {@link Clock} used for testing purpose.
 * It provides the desired {@link Instant} invoking the specified supplier.
 *
 * @author Sara Pellegrini
 */
public class FakeClock extends Clock {

    private final Supplier<Instant> instant;
    private final ZoneId zone;

    public FakeClock(Supplier<Instant> instant) {
        this(instant, ZoneId.systemDefault());
    }

    public FakeClock(Supplier<Instant> instant, ZoneId zone) {
        this.instant = instant;
        this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        if (zone.equals(this.zone)) {
            return this;
        }
        return new FakeClock(instant, zone);
    }

    @Override
    public Instant instant() {
        return instant.get();
    }

    public FakeClock plusMillis(long millis) {
        return new FakeClock(() -> instant.get().plusMillis(millis), zone);
    }
}
