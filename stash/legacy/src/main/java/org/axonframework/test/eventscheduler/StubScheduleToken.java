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

package org.axonframework.test.eventscheduler;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.scheduling.ScheduleToken;

import java.time.Instant;
import java.util.Objects;


/**
 * ScheduleToken returned by the StubEventScheduler.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class StubScheduleToken implements ScheduleToken, Comparable<StubScheduleToken>, ScheduledItem {

    private final Instant scheduleTime;
    private final EventMessage event;
    private final int counter;

    /**
     * Initialize the token with the given {@code scheduleTime}, {@code event} and {@code counter}.
     *
     * @param scheduleTime The time at which to trigger the event
     * @param event        The scheduled event
     * @param counter      A counter used for sorting purposes. When two events are scheduled for the same time, the
     *                     counter decides which comes first.
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
            return (counter < other.counter) ? -1 : ((counter == other.counter) ? 0 : 1);
        }
        return scheduleTime.compareTo(other.scheduleTime);
    }

    @Override
    public boolean equals(Object o) {
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
