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

package org.axonframework.test.saga;

import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.UUID;

/**
 * Test class validating the event scheduler operations of the {@link SagaTestFixture}.
 * <p>
 * Every case is commented out, because the event scheduler is not ported into {@code axoniq-legacy} yet and the
 * assertions they use throw. The Axon Framework 4 source is kept here rather than deleted, so porting the scheduler is
 * a matter of uncommenting and running.
 *
 * @author Steven van Beelen
 */
@Disabled("#3104 - the event scheduler is not ported into axoniq-legacy yet")
class FixtureScheduledEventsTest {

    private static final String IDENTIFIER = UUID.randomUUID().toString();
    private static final Duration TRIGGER_DURATION_MINUTES = Duration.ofMinutes(10);
    private static final String EVENT_TO_SCHEDULE = "scheduled-event";

    private FixtureConfiguration testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new SagaTestFixture<>(EventSchedulingSaga.class);
    }

//    @Test
//    void expectScheduledEvent() {
//        testSubject.givenNoPriorActivity()
//                   .whenPublishingA(new TriggerSagaStartEvent(IDENTIFIER))
//                   .expectScheduledEvent(TRIGGER_DURATION_MINUTES, EVENT_TO_SCHEDULE);
//    }
//
//    @Test
//    void expectScheduledEventOfType() {
//        testSubject.givenNoPriorActivity()
//                   .whenPublishingA(new TriggerSagaStartEvent(IDENTIFIER))
//                   .expectScheduledEventOfType(TRIGGER_DURATION_MINUTES, String.class);
//    }
//
//    @Test
//    void expectScheduledEventDurationAdjustedByElapsedTime() {
//        Duration elapsedTime = Duration.ofMinutes(1);
//
//        testSubject.givenAggregate(IDENTIFIER)
//                   .published(new TriggerSagaStartEvent(IDENTIFIER))
//                   .whenTimeElapses(elapsedTime)
//                   .expectScheduledEvent(TRIGGER_DURATION_MINUTES.minus(elapsedTime), EVENT_TO_SCHEDULE)
//                   .expectNoScheduledDeadlines();
//    }
//
//    @Test
//    void noExpectScheduledEvent() {
//        testSubject.givenAggregate(IDENTIFIER)
//                   .published(new TriggerSagaStartEvent(IDENTIFIER))
//                   .whenAggregate(IDENTIFIER)
//                   .publishes(new CancelScheduledTokenEvent(IDENTIFIER))
//                   .expectNoScheduledEvent(TRIGGER_DURATION_MINUTES, EVENT_TO_SCHEDULE);
//    }
//
//    @Test
//    void noExpectScheduledEventOfType() {
//        testSubject.givenAggregate(IDENTIFIER)
//                   .published(new TriggerSagaStartEvent(IDENTIFIER))
//                   .whenAggregate(IDENTIFIER)
//                   .publishes(new CancelScheduledTokenEvent(IDENTIFIER))
//                   .expectNoScheduledEventOfType(TRIGGER_DURATION_MINUTES, String.class);
//    }

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

    @SuppressWarnings("unused")
    public static class EventSchedulingSaga {

//        @Inject
//        private transient EventScheduler scheduler;
//        private ScheduleToken scheduleToken;

//        @StartSaga
//        @SagaEventHandler(associationProperty = "identifier")
//        public void on(TriggerSagaStartEvent event, @Timestamp Instant timestamp) {
//            scheduleToken = scheduler.schedule(timestamp.plus(TRIGGER_DURATION_MINUTES), EVENT_TO_SCHEDULE);
//        }
//
//        @SagaEventHandler(associationProperty = "identifier")
//        public void on(CancelScheduledTokenEvent event) {
//            scheduler.cancelSchedule(scheduleToken);
//        }
    }
}
