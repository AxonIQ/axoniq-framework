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

package org.axonframework.messaging.eventhandling.scheduling.java;

import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.scheduling.ScheduleToken;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.scheduling.java.SimpleEventScheduler;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests validating the {@link SimpleEventScheduler}.
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 */
@ExtendWith(MockitoExtension.class)
class SimpleEventSchedulerTest {

    private ScheduledExecutorService scheduledExecutorService;
    private EventBus eventBus;

    private SimpleEventScheduler testSubject;

    @BeforeEach
    void setUp() {
        eventBus = mock(EventBus.class);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        testSubject = SimpleEventScheduler.builder()
                                          .scheduledExecutorService(scheduledExecutorService)
                                          .eventBus(eventBus)
                                          .build();
    }

    @AfterEach
    void tearDown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Test
    void scheduleJob() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(eventBus).publish(eq(null), isA(EventMessage.class));
        testSubject.schedule(Duration.ofMillis(30), new Object());
        latch.await(1, TimeUnit.SECONDS);
        verify(eventBus).publish(eq(null), isA(EventMessage.class));
    }

    @Test
    void cancelJob() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(eventBus).publish(eq(null), isA(EventMessage.class));
        EventMessage event1 = createEvent();
        final EventMessage event2 = createEvent();
        ScheduleToken token1 = testSubject.schedule(Duration.ofMillis(100), event1);
        testSubject.schedule(Duration.ofMillis(120), event2);
        testSubject.cancelSchedule(token1);
        latch.await(1, TimeUnit.SECONDS);
        verify(eventBus, never()).publish(null, event1);
        verify(eventBus).publish(eq(null), argThat((ArgumentMatcher<EventMessage>) item -> (item != null)
                && event2.payload().equals(item.payload())
                && event2.metadata().equals(item.metadata())));
        scheduledExecutorService.shutdown();
        assertTrue(scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS),
                   "Executor refused to shutdown within a second");
    }

    @Test
    void shutdownInvokesExecutorServiceShutdown(@Mock ScheduledExecutorService scheduledExecutorService) {
        SimpleEventScheduler testSubject = SimpleEventScheduler.builder()
                                                               .scheduledExecutorService(scheduledExecutorService)
                                                               .eventBus(eventBus)
                                                               .build();

        testSubject.shutdown();

        verify(scheduledExecutorService).shutdown();
    }

    private EventMessage createEvent() {
        return new GenericEventMessage(new MessageType("event"), new Object());
    }
}
