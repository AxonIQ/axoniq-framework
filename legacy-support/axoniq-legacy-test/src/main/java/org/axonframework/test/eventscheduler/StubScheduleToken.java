/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.eventscheduler;

import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.io.Serial;
import java.time.Instant;
import java.util.Objects;

/**
 * {@link ScheduleToken} returned by the {@link StubEventScheduler}.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class StubScheduleToken implements ScheduleToken, Comparable<StubScheduleToken>, ScheduledItem {

    @Serial
    private static final long serialVersionUID = 3763093001261110665L;

    private final Instant scheduleTime;
    private final EventMessage event;
    private final int counter;

    /**
     * Initialize the token with the given {@code scheduleTime}, {@code event} and {@code counter}.
     *
     * @param scheduleTime the time at which to trigger the event
     * @param event        the scheduled event
     * @param counter      a counter used for sorting purposes; when two events are scheduled for the same time, the
     *                     counter decides which comes first
     */
    StubScheduleToken(Instant scheduleTime, EventMessage event, int counter) {
        this.scheduleTime = scheduleTime;
        this.event = event;
        this.counter = counter;
    }

    @Override
    public Instant getScheduleTime() {
        return scheduleTime;
    }

    @Override
    public EventMessage getEvent() {
        return new GenericEventMessage(event, () -> scheduleTime);
    }

    @Override
    public int compareTo(StubScheduleToken other) {
        if (scheduleTime.equals(other.scheduleTime)) {
            return Integer.compare(counter, other.counter);
        }
        return scheduleTime.compareTo(other.scheduleTime);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StubScheduleToken that = (StubScheduleToken) o;
        return counter == that.counter && Objects.equals(scheduleTime, that.scheduleTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheduleTime, counter);
    }
}
