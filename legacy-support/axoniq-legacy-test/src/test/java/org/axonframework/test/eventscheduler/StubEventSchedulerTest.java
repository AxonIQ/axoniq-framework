/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.eventscheduler;

import org.axonframework.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link StubEventScheduler}.
 *
 * @author Allard Buijze
 * @author Jakob Hatzl
 */
@SuppressWarnings("deprecation")
class StubEventSchedulerTest {

    private StubEventScheduler testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new StubEventScheduler();
    }

    @Nested
    class Schedule {

        @Test
        void scheduleEvent() {
            // when
            testSubject.schedule(Instant.now().plus(Duration.ofDays(1)), event(new MockEvent()));

            // then
            assertThat(testSubject.getScheduledItems()).hasSize(1);
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void schedulingRejectsNullEvent() {
            // when / then
            assertThatThrownBy(() -> testSubject.schedule(Duration.ofMinutes(1), null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.schedule(Instant.now(), null))
                    .isInstanceOf(NullPointerException.class);
            assertThat(testSubject.getScheduledItems()).isEmpty();
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void schedulingRejectsNullTrigger() {
            // when / then
            assertThatThrownBy(() -> testSubject.schedule((Duration) null, "payload"))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.schedule((Instant) null, "payload"))
                    .isInstanceOf(NullPointerException.class);
            assertThat(testSubject.getScheduledItems()).isEmpty();
        }

        @Test
        void eventContainsTimestampOfScheduledTime() {
            // given
            Instant triggerTime = Instant.now().plusSeconds(60);
            testSubject.schedule(triggerTime, "gone");

            // when
            List<EventMessage> triggered = new ArrayList<>();
            testSubject.advanceTimeBy(Duration.ofMinutes(75), triggered::add);

            // then
            assertThat(triggered).hasSize(1);
            assertThat(triggered.getFirst().timestamp()).isEqualTo(triggerTime);
        }

        @Test
        void scheduledPayloadGetsMessageTypeFromEventAnnotationByDefault() {
            // given
            testSubject.schedule(Duration.ofMinutes(1), new NamedEvent("payload"));

            // when
            EventMessage result = testSubject.advanceToNextTrigger();

            // then
            assertThat(result.type()).isEqualTo(new MessageType("scheduling.NamedEvent", "2"));
        }

        @Test
        void scheduledPayloadGetsMessageTypeOfConfiguredResolver() {
            // given
            MessageType customType = new MessageType("custom.Event");
            StubEventScheduler customSubject =
                    new StubEventScheduler(Instant.now(), payloadType -> Optional.of(customType));
            customSubject.schedule(Duration.ofMinutes(1), "payload");

            // when
            EventMessage result = customSubject.advanceToNextTrigger();

            // then
            assertThat(result.type()).isEqualTo(customType);
            assertThat(result.payload()).isEqualTo("payload");
        }

        @Test
        void scheduledEventMessageKeepsItsMessageType() {
            // given
            EventMessage scheduled = event(new MockEvent());
            testSubject.schedule(Duration.ofMinutes(1), scheduled);

            // when
            EventMessage result = testSubject.advanceToNextTrigger();

            // then
            assertThat(result.type()).isEqualTo(scheduled.type());
        }

        @Test
        void eventsScheduledForSameTimeAreTriggeredInSchedulingOrder() {
            // given
            Instant triggerTime = Instant.now().plusSeconds(60);
            testSubject.schedule(triggerTime, "first");
            testSubject.schedule(triggerTime, "second");

            // when
            List<EventMessage> triggered = new ArrayList<>();
            testSubject.advanceTimeTo(triggerTime, triggered::add);

            // then
            assertThat(triggered).extracting(EventMessage::payload).containsExactly("first", "second");
        }
    }

    @Nested
    class InitializeAt {

        @Test
        void initializeAtDateTimeAfterSchedulingEvent() {
            // given
            testSubject.schedule(Instant.now().plus(Duration.ofDays(1)), event(new MockEvent()));

            // when / then
            assertThatThrownBy(() -> testSubject.initializeAt(Instant.now().minus(10, ChronoUnit.MINUTES)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class CancelSchedule {

        @Test
        void cancelledEventIsNotTriggered() {
            // given
            ScheduleToken token = testSubject.schedule(Duration.ofMinutes(1), "cancelled");
            testSubject.schedule(Duration.ofMinutes(2), "kept");

            // when
            testSubject.cancelSchedule(token);

            // then
            List<EventMessage> triggered = new ArrayList<>();
            testSubject.advanceTimeBy(Duration.ofMinutes(5), triggered::add);
            assertThat(triggered).extracting(EventMessage::payload).containsExactly("kept");
        }

        @Test
        void cancellingForeignTokenThrowsIllegalArgumentException() {
            // given
            ScheduleToken foreignToken = new ScheduleToken() {
            };

            // when / then
            assertThatThrownBy(() -> testSubject.cancelSchedule(foreignToken))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static EventMessage event(MockEvent mockEvent) {
        return new GenericEventMessage(new MessageType("event"), mockEvent);
    }

    @Event(namespace = "scheduling", name = "NamedEvent", version = "2")
    private record NamedEvent(String value) {

    }

    private static class MockEvent {

    }
}
