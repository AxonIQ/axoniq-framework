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
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link StubEventScheduler}.
 *
 * @author Allard Buijze
 */
class StubEventSchedulerTest {

    private StubEventScheduler testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new StubEventScheduler();
    }

    @Test
    void scheduleEvent() {
        testSubject.schedule(Instant.now().plus(Duration.ofDays(1)), event(new MockEvent()));
        assertEquals(1, testSubject.getScheduledItems().size());
    }

    @Test
    void eventContainsTimestampOfScheduledTime() {
        Instant triggerTime = Instant.now().plusSeconds(60);
        testSubject.schedule(triggerTime, "gone");
        List<EventMessage> triggered = new ArrayList<>();
        testSubject.advanceTimeBy(Duration.ofMinutes(75), triggered::add);

        assertEquals(1, triggered.size());
        assertEquals(triggerTime, triggered.getFirst().timestamp());
    }

    @Test
    void initializeAtDateTimeAfterSchedulingEvent() {
        testSubject.schedule(Instant.now().plus(Duration.ofDays(1)), event(new MockEvent()));

        assertThrows(IllegalStateException.class,
                     () -> testSubject.initializeAt(Instant.now().minus(10, ChronoUnit.MINUTES)));
    }

    private EventMessage event(MockEvent mockEvent) {
        return new GenericEventMessage(new MessageType("event"), mockEvent);
    }

    private static class MockEvent {

    }
}
