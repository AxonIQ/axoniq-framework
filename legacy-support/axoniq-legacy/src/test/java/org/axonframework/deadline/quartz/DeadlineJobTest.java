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

package org.axonframework.deadline.quartz;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.messaging.ScopeAware;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating how the {@link DeadlineJob} handles an {@link Error} thrown while delivering the deadline.
 *
 * @author Jakob Hatzl
 */
class DeadlineJobTest {

    private static final ScopeDescriptor SCOPE = new TestScopeDescriptor("aggregateType", "identifier");
    private static final Duration FIRING_TIMEOUT = Duration.ofSeconds(5);

    private final List<Object> deliveredPayloads = new CopyOnWriteArrayList<>();
    private final List<Object> attempts = new CopyOnWriteArrayList<>();
    private final AtomicReference<@Nullable Throwable> failure = new AtomicReference<>();

    private Scheduler scheduler;

    @BeforeEach
    void setUp() throws SchedulerException {
        Properties properties = new Properties();
        properties.setProperty("org.quartz.scheduler.instanceName", "DeadlineJobTest-" + UUID.randomUUID());
        properties.setProperty("org.quartz.threadPool.threadCount", "2");
        properties.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
        scheduler = new StdSchedulerFactory(properties).getScheduler();
        scheduler.start();
    }

    @AfterEach
    void tearDown() throws SchedulerException {
        scheduler.shutdown(true);
    }

    @Test
    void anErrorDuringDeliveryIsHandedToTheRefirePolicy() {
        // given: the default policy refires everything but an AxonNonTransientException
        failure.set(new AssertionError("failing once"));
        QuartzDeadlineManager deadlineManager = managerOn(scheduler);

        // when
        deadlineManager.schedule(Duration.ofMillis(10), "deadline", "payload", SCOPE);

        // then
        await().atMost(FIRING_TIMEOUT)
               .untilAsserted(() -> assertThat(deliveredPayloads).containsExactly("payload"));
        assertThat(attempts).hasSize(2);
    }

    private QuartzDeadlineManager managerOn(Scheduler scheduler) {
        return QuartzDeadlineManager.builder()
                                    .scheduler(scheduler)
                                    .scopeAwareProvider(scope -> Stream.of(new RecordingScopeAware()))
                                    .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                    .converter(new JacksonConverter())
                                    .build();
    }

    private class RecordingScopeAware implements ScopeAware {

        @Override
        public void send(Message message, ProcessingContext context, ScopeDescriptor scopeDescription) {
            attempts.add(message.payload());
            Throwable toThrow = failure.getAndSet(null);
            if (toThrow instanceof Error error) {
                throw error;
            }
            deliveredPayloads.add(message.payload());
        }

        @Override
        public boolean canResolve(ScopeDescriptor scopeDescription) {
            return true;
        }
    }
}
