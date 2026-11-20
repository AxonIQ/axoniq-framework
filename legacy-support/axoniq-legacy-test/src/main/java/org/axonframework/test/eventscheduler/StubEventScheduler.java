/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.eventscheduler;

import org.axonframework.eventhandling.scheduling.EventScheduler;
import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EventScheduler implementation that uses its own concept of "Current Time" for the purpose of testing. Instead of
 * publishing events, it allows (test) classes to obtain a reference to scheduled events for a certain time frame.
 * <p>
 * To obtain scheduled events, the conceptual "current time" of the Scheduler must be advanced. All events that have
 * been scheduled within the advanced time frame are returned. It is up to the calling class to decide what to do with
 * these events.
 * <p>
 * A scheduled payload that is no {@link EventMessage} is wrapped in one, with the
 * {@link org.axonframework.messaging.core.MessageType} the configured {@link MessageTypeResolver} resolves for it.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class StubEventScheduler implements EventScheduler {

    private final NavigableSet<StubScheduleToken> scheduledEvents = new TreeSet<>();
    private final AtomicInteger counter = new AtomicInteger(0);
    private final MessageTypeResolver messageTypeResolver;
    private Instant currentDateTime;

    /**
     * Creates an instance of the StubScheduler that uses the current date time as its conceptual "current time".
     * Unlike the real "current time", the time of the Event Scheduler is fixed.
     */
    public StubEventScheduler() {
        this(ZonedDateTime.now());
    }

    /**
     * Creates an instance of the StubScheduler that uses the given {@code currentDateTime} as its conceptual "current
     * time", and an {@link AnnotationMessageTypeResolver}, the configuration's default, for scheduled payloads.
     *
     * @param currentDateTime the instant to use as current Date and Time
     */
    public StubEventScheduler(TemporalAccessor currentDateTime) {
        this(currentDateTime, new AnnotationMessageTypeResolver());
    }

    /**
     * Creates an instance of the StubScheduler that uses the given {@code currentDateTime} as its conceptual "current
     * time", and the given {@code messageTypeResolver} for scheduled payloads.
     *
     * @param currentDateTime     the instant to use as current Date and Time
     * @param messageTypeResolver the resolver of the message type of a scheduled payload that is no
     *                            {@link EventMessage}
     */
    public StubEventScheduler(TemporalAccessor currentDateTime, MessageTypeResolver messageTypeResolver) {
        this.currentDateTime = Instant.from(currentDateTime);
        this.messageTypeResolver =
                Objects.requireNonNull(messageTypeResolver, "The MessageTypeResolver may not be null.");
    }

    /**
     * Resets the initial "current time" of this StubEventScheduler. Must be called before any events are scheduled.
     *
     * @param currentDateTime the instant to use as the current Date and Time
     * @throws IllegalStateException when calling this method after events are scheduled
     */
    public void initializeAt(TemporalAccessor currentDateTime) {
        if (!scheduledEvents.isEmpty()) {
            throw new IllegalStateException("Initializing the scheduler at a specific dateTime must take place "
                                                    + "before any events are scheduled");
        }
        this.currentDateTime = Instant.from(currentDateTime);
    }

    private EventMessage asEventMessage(Object event) {
        if (event instanceof EventMessage eventMessage) {
            return eventMessage;
        } else if (event instanceof Message message) {
            return new GenericEventMessage(message);
        }
        return new GenericEventMessage(messageTypeResolver.resolveOrThrow(event), event);
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated scheduling new events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Override
    @Deprecated(since = "5.5.0")
    public ScheduleToken schedule(Instant triggerDateTime, Object event) {
        Objects.requireNonNull(triggerDateTime, "The trigger date time may not be null.");
        Objects.requireNonNull(event, "The event may not be null.");
        EventMessage eventMessage = asEventMessage(event);
        StubScheduleToken token = new StubScheduleToken(triggerDateTime, eventMessage, counter.getAndIncrement());
        scheduledEvents.add(token);
        return token;
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated scheduling new events through an {@code EventScheduler} is supported only to keep Axon Framework 4
     *             code running while migrating. Schedule with a scheduler of choice that publishes through the
     *             {@link org.axonframework.messaging.eventhandling.gateway.EventGateway} instead
     */
    @Override
    @Deprecated(since = "5.5.0")
    public ScheduleToken schedule(Duration triggerDuration, Object event) {
        Objects.requireNonNull(triggerDuration, "The trigger duration may not be null.");
        Objects.requireNonNull(event, "The event may not be null.");
        EventMessage eventMessage = asEventMessage(event);
        Instant scheduleTime = currentDateTime.plus(triggerDuration);
        StubScheduleToken token = new StubScheduleToken(scheduleTime, eventMessage, counter.getAndIncrement());
        scheduledEvents.add(token);
        return token;
    }

    @Override
    public void cancelSchedule(ScheduleToken scheduleToken) {
        if (!(scheduleToken instanceof StubScheduleToken stubToken)) {
            throw new IllegalArgumentException("Wrong token type. This token was not provided by this scheduler");
        }
        scheduledEvents.remove(stubToken);
    }

    /**
     * Returns a view of all the scheduled Events at the time this method is called.
     *
     * @return a view of all the scheduled Events at the time this method is called
     */
    public List<ScheduledItem> getScheduledItems() {
        return new ArrayList<>(scheduledEvents);
    }

    /**
     * Returns the "Current Date Time" as used by the scheduler.
     *
     * @return the "Current Date Time" as used by the scheduler
     */
    public Instant getCurrentDateTime() {
        return currentDateTime;
    }

    /**
     * Advances the "current time" of the scheduler to the next scheduled Event, and returns that event. In theory, this
     * may cause "current time" to move backwards.
     *
     * @return the first event scheduled
     */
    public EventMessage advanceToNextTrigger() {
        StubScheduleToken nextItem = scheduledEvents.pollFirst();
        if (nextItem == null) {
            throw new NoSuchElementException("There are no scheduled events");
        }
        if (nextItem.getScheduleTime().isAfter(currentDateTime)) {
            currentDateTime = nextItem.getScheduleTime();
        }
        return nextItem.getEvent();
    }

    /**
     * Advance time to the given {@code newDateTime} and invokes the given {@code eventConsumer} for each
     * event scheduled for publication until that time.
     *
     * @param newDateTime   the time to advance the "current time" of the scheduler to
     * @param eventConsumer the function to invoke for each event to trigger
     */
    public void advanceTimeTo(Instant newDateTime, EventConsumer<EventMessage> eventConsumer) {
        while (!scheduledEvents.isEmpty() && !scheduledEvents.first().getScheduleTime().isAfter(newDateTime)) {
            eventConsumer.accept(advanceToNextTrigger());
        }
        if (newDateTime.isAfter(currentDateTime)) {
            currentDateTime = newDateTime;
        }
    }

    /**
     * Advance time by the given {@code duration} and invokes the given {@code eventConsumer} for each
     * event scheduled for publication until that time.
     *
     * @param duration      the amount of time to advance the "current time" of the scheduler with
     * @param eventConsumer the function to invoke for each event to trigger
     */
    public void advanceTimeBy(Duration duration, EventConsumer<EventMessage> eventConsumer) {
        advanceTimeTo(currentDateTime.plus(duration), eventConsumer);
    }
}
