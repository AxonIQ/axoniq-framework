/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.saga;

import org.axonframework.eventhandling.scheduling.EventScheduler;
import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.eventhandling.annotation.Timestamp;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.test.AxonAssertionError;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the event scheduler operations of the {@link SagaTestFixture}.
 *
 * @author Steven van Beelen
 */
class FixtureScheduledEventsTest {

    private static final String IDENTIFIER = UUID.randomUUID().toString();
    private static final Duration TRIGGER_DURATION_MINUTES = Duration.ofMinutes(10);
    private static final Duration FAILING_TRIGGER_DURATION = Duration.ofMinutes(1);
    private static final ScheduledSagaEndEvent EVENT_TO_SCHEDULE = new ScheduledSagaEndEvent(IDENTIFIER);

    private SagaTestFixture<EventSchedulingSaga> testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new SagaTestFixture<>(EventSchedulingSaga.class);
    }

    @AfterEach
    void tearDown() {
        testSubject.stop();
    }

    @Test
    void expectScheduledEvent() {
        testSubject.givenNoPriorActivity()
                   .whenPublishingA(new TriggerSagaStartEvent(IDENTIFIER))
                   .expectScheduledEvent(TRIGGER_DURATION_MINUTES, EVENT_TO_SCHEDULE);
    }

    @Test
    void expectScheduledEventOfType() {
        testSubject.givenNoPriorActivity()
                   .whenPublishingA(new TriggerSagaStartEvent(IDENTIFIER))
                   .expectScheduledEventOfType(TRIGGER_DURATION_MINUTES, ScheduledSagaEndEvent.class);
    }

    @Test
    void expectScheduledEventDurationAdjustedByElapsedTime() {
        Duration elapsedTime = Duration.ofMinutes(1);

        testSubject.givenAggregate(IDENTIFIER)
                   .published(new TriggerSagaStartEvent(IDENTIFIER))
                   .whenTimeElapses(elapsedTime)
                   .expectScheduledEvent(TRIGGER_DURATION_MINUTES.minus(elapsedTime), EVENT_TO_SCHEDULE)
                   .expectNoScheduledDeadlines();
    }

    @Test
    void noExpectScheduledEvent() {
        testSubject.givenAggregate(IDENTIFIER)
                   .published(new TriggerSagaStartEvent(IDENTIFIER))
                   .whenAggregate(IDENTIFIER)
                   .publishes(new CancelScheduledTokenEvent(IDENTIFIER))
                   .expectNoScheduledEvent(TRIGGER_DURATION_MINUTES, EVENT_TO_SCHEDULE);
    }

    @Test
    void noExpectScheduledEventOfType() {
        testSubject.givenAggregate(IDENTIFIER)
                   .published(new TriggerSagaStartEvent(IDENTIFIER))
                   .whenAggregate(IDENTIFIER)
                   .publishes(new CancelScheduledTokenEvent(IDENTIFIER))
                   .expectNoScheduledEventOfType(TRIGGER_DURATION_MINUTES, ScheduledSagaEndEvent.class);
    }

    @Test
    void scheduledEventIsHandledBySagaWhenTimeElapsesButNotReportedAsPublished() {
        testSubject.givenAggregate(IDENTIFIER)
                   .published(new TriggerSagaStartEvent(IDENTIFIER))
                   .whenTimeElapses(TRIGGER_DURATION_MINUTES)
                   .expectSuccessfulHandlerExecution()
                   .expectNoScheduledEvents()
                   .expectPublishedEvents()
                   // the Saga handled the scheduled event, which ends it
                   .expectActiveSagas(0);
    }

    @Test
    void sagaFailingToHandleScheduledEventIsReported() {
        // given
        ContinuedGivenState given = testSubject.givenAggregate(IDENTIFIER)
                                               .published(new TriggerSagaStartEvent(IDENTIFIER),
                                                          new TriggerFailingScheduledEvent(IDENTIFIER));

        // when
        FixtureExecutionResult result = given.whenTimeElapses(FAILING_TRIGGER_DURATION);

        // then
        assertThatThrownBy(result::expectSuccessfulHandlerExecution).isInstanceOf(AxonAssertionError.class);
    }

    @Test
    void sagaFailingToHandleEarlierScheduledEventIsReportedWhenLaterOneSucceeds() {
        // given
        ContinuedGivenState given = testSubject.givenAggregate(IDENTIFIER)
                                               .published(new TriggerSagaStartEvent(IDENTIFIER),
                                                          new TriggerFailingScheduledEvent(IDENTIFIER));

        // when
        // publishes the failing event first, and then the event ending the Saga
        FixtureExecutionResult result = given.whenTimeElapses(TRIGGER_DURATION_MINUTES);

        // then
        result.expectActiveSagas(0);
        assertThatThrownBy(result::expectSuccessfulHandlerExecution).isInstanceOf(AxonAssertionError.class);
    }

    @Test
    void scheduledEventIsPublishedWhenTimeElapsesInGivenPhase() {
        testSubject.givenAggregate(IDENTIFIER)
                   .published(new TriggerSagaStartEvent(IDENTIFIER))
                   .andThenTimeElapses(TRIGGER_DURATION_MINUTES)
                   .whenPublishingA(new TriggerSagaStartEvent(IDENTIFIER))
                   .expectActiveSagas(1)
                   .expectNoScheduledEvent(Duration.ZERO, EVENT_TO_SCHEDULE)
                   .expectScheduledEvent(TRIGGER_DURATION_MINUTES, EVENT_TO_SCHEDULE);
    }

    private static class TriggerFailingScheduledEvent {

        private final String identifier;

        public TriggerFailingScheduledEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }
    }

    private static class FailingScheduledEvent {

        private final String identifier;

        public FailingScheduledEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }
    }

    private static class ScheduledSagaEndEvent {

        private final String identifier;

        public ScheduledSagaEndEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ScheduledSagaEndEvent other && identifier.equals(other.identifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identifier);
        }
    }

    private static class CancelScheduledTokenEvent {

        @SuppressWarnings("unused")
        private String identifier;

        public CancelScheduledTokenEvent(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }
    }

    /**
     * Schedules a {@link ScheduledSagaEndEvent} when it starts, and ends when that event is published to it. Fails to
     * handle a {@link FailingScheduledEvent}, which it schedules on a {@link TriggerFailingScheduledEvent}.
     * <p>
     * Axon Framework 4 injected the {@link EventScheduler} into a field. Axon Framework 5 dropped field injection, so
     * the handlers declare it as a parameter.
     */
    @SuppressWarnings({"unused", "removal", "deprecation"})
    public static class EventSchedulingSaga {

        private ScheduleToken scheduleToken;

        @StartSaga
        @SagaEventHandler(associationProperty = "identifier")
        public void on(TriggerSagaStartEvent event, @Timestamp Instant timestamp, EventScheduler scheduler) {
            scheduleToken = scheduler.schedule(timestamp.plus(TRIGGER_DURATION_MINUTES), EVENT_TO_SCHEDULE);
        }

        @SagaEventHandler(associationProperty = "identifier")
        public void on(CancelScheduledTokenEvent event, EventScheduler scheduler) {
            scheduler.cancelSchedule(scheduleToken);
        }

        @SagaEventHandler(associationProperty = "identifier")
        public void on(TriggerFailingScheduledEvent event, @Timestamp Instant timestamp, EventScheduler scheduler) {
            scheduler.schedule(timestamp.plus(FAILING_TRIGGER_DURATION),
                               new FailingScheduledEvent(event.getIdentifier()));
        }

        @SagaEventHandler(associationProperty = "identifier")
        public void on(FailingScheduledEvent event) {
            throw new IllegalStateException("Failing to handle the scheduled event");
        }

        @EndSaga
        @SagaEventHandler(associationProperty = "identifier")
        public void on(ScheduledSagaEndEvent event) {
            // ends the Saga
        }
    }
}
