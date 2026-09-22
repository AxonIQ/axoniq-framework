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
package io.axoniq.framework.workflow.runtime.test.utils;

import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DelayedPublisher}.
 *
 * @author Simon Zambrovski
 */
class DelayedPublisherTest {

    @Test
    void startPublishesSchedulesInOrder() {
        TestEventPublisher eventPublisher = mock(TestEventPublisher.class);
        when(eventPublisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
        Executor directExecutor = Runnable::run;
        DelayedPublisher publisher = new DelayedPublisher(eventPublisher, directExecutor);
        Object first = new Object();
        Object second = new Object();

        publisher.addSchedules(List.of(DelayedPublisher.Schedule.ofMillis(0, first),
                                       DelayedPublisher.Schedule.of(Duration.ZERO, second)));
        publisher.start().join();

        InOrder inOrder = inOrder(eventPublisher);
        inOrder.verify(eventPublisher).publish(first);
        inOrder.verify(eventPublisher).publish(second);
    }

    @Test
    void startWithoutSchedulesCompletesImmediately() {
        DelayedPublisher publisher = new DelayedPublisher(mock(TestEventPublisher.class), Runnable::run);

        CompletableFuture<Void> future = publisher.start();

        assertThat(future).isCompleted();
    }

    @Test
    void scheduleFactoryMethodsSetDurationAndEvent() {
        Object event = new Object();

        DelayedPublisher.Schedule schedule = DelayedPublisher.Schedule.ofMillis(25, event);

        assertThat(schedule.duration()).isEqualTo(Duration.ofMillis(25));
        assertThat(schedule.event()).isSameAs(event);
    }
}
