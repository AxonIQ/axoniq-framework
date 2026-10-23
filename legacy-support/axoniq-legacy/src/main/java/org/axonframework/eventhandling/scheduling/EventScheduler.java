/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;

/**
 * Interface towards a mechanism capable of scheduling the publication of events. The accuracy of the publication time
 * depends on the exact implementation used.
 * <p>
 * This contract exists to keep events that were scheduled with Axon Framework 4 firing while migrating, and to cancel
 * them. Scheduling and rescheduling events is therefore deprecated, while cancelling them stays fully supported. New
 * code schedules with a scheduler of choice that publishes through the
 * {@link org.axonframework.messaging.eventhandling.gateway.EventGateway}, as the
 * <a href="https://docs.axoniq.io/message-scheduling-guide">Message Scheduling guide</a> describes.
 * <p>
 * Calls take effect immediately, as in Axon Framework 4: they are not deferred to the commit of a unit of work that
 * may be active. An event scheduled from a handler whose unit of work rolls back thus stays scheduled.
 * <p>
 * A fired event is a new event message, with a new identifier and a timestamp of the moment it fired. Its payload and
 * metadata are those of the scheduled event.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public interface EventScheduler {

    /**
     * Schedule the given {@code event} for publication at the given {@code triggerDateTime}. The returned
     * {@link ScheduleToken} can be used to cancel the planned publication.
     * <p>
     * The given {@code event} may be any object, as well as an
     * {@link org.axonframework.messaging.eventhandling.EventMessage}. In the latter case, the instance provided is the
     * donor for the payload and metadata of the actual message being published. In the former case, the given
     * {@code event} will be wrapped as the payload of an event message.
     *
     * @param triggerDateTime the moment to trigger publication of the event
     * @param event           the event to publish
     * @return the token to use when cancelling the schedule
     * @deprecated scheduling new events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Deprecated(since = "5.5.0")
    ScheduleToken schedule(Instant triggerDateTime, Object event);

    /**
     * Schedule the given {@code event} for publication after the given {@code triggerDuration}. The returned
     * {@link ScheduleToken} can be used to cancel the planned publication.
     * <p>
     * The given {@code event} may be any object, as well as an
     * {@link org.axonframework.messaging.eventhandling.EventMessage}. In the latter case, the instance provided is the
     * donor for the payload and metadata of the actual message being published. In the former case, the given
     * {@code event} will be wrapped as the payload of an event message.
     *
     * @param triggerDuration the amount of time to wait before publishing the event
     * @param event           the event to publish
     * @return the token to use when cancelling the schedule
     * @deprecated scheduling new events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Deprecated(since = "5.5.0")
    ScheduleToken schedule(Duration triggerDuration, Object event);

    /**
     * Cancel the publication of a scheduled event. If the event has already been published, this method does nothing.
     *
     * @param scheduleToken the token returned when the event was scheduled
     * @throws IllegalArgumentException if the token belongs to another scheduler
     */
    void cancelSchedule(ScheduleToken scheduleToken);

    /**
     * Cancel a scheduled event and schedule another in its place.
     * <p>
     * Convenience method around {@link #cancelSchedule(ScheduleToken)} and {@link #schedule(Duration, Object)}.
     *
     * @param scheduleToken   the token returned when the event was scheduled, might be {@code null}
     * @param triggerDuration the amount of time to wait before publishing the event
     * @param event           the event to publish
     * @return the token to use when cancelling the schedule
     * @throws IllegalArgumentException if the token belongs to another scheduler
     * @deprecated rescheduling events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Cancel the event with {@link #cancelSchedule(ScheduleToken)} and
     *             schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Deprecated(since = "5.5.0")
    default ScheduleToken reschedule(@Nullable ScheduleToken scheduleToken, Duration triggerDuration, Object event) {
        if (null != scheduleToken) {
            cancelSchedule(scheduleToken);
        }
        return schedule(triggerDuration, event);
    }

    /**
     * Cancel a scheduled event and schedule another in its place.
     * <p>
     * Convenience method around {@link #cancelSchedule(ScheduleToken)} and {@link #schedule(Instant, Object)}.
     *
     * @param scheduleToken the token returned when the event was scheduled, might be {@code null}
     * @param instant       the moment to trigger publication of the event
     * @param event         the event to publish
     * @return the token to use when cancelling the schedule
     * @throws IllegalArgumentException if the token belongs to another scheduler
     * @deprecated rescheduling events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Cancel the event with {@link #cancelSchedule(ScheduleToken)} and
     *             schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Deprecated(since = "5.5.0")
    default ScheduleToken reschedule(@Nullable ScheduleToken scheduleToken, Instant instant, Object event) {
        if (null != scheduleToken) {
            cancelSchedule(scheduleToken);
        }
        return schedule(instant, event);
    }

    /**
     * Shuts down this event scheduler. Implementations allow repeated calls.
     */
    default void shutdown() {
    }
}
