/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.quartz;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.messaging.ScopeAware;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.listeners.JobListenerSupport;

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
 * Test class validating how the {@link DeadlineJob} handles failures that are not those of the deadline's handler: a
 * job firing before the {@link QuartzDeadlineManager} filled the scheduler's context, and an {@link Error} thrown while
 * delivering the deadline.
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
    void aJobFiringBeforeTheManagerFilledTheSchedulerContextFailsWithoutRefiring() throws SchedulerException {
        // given
        StoredDeadlineConverter converter = new StoredDeadlineConverter(new JacksonConverter());
        GenericDeadlineMessage deadline =
                new GenericDeadlineMessage("deadline", StoredDeadlineConverter.messageTypeOf("payload"), "payload");
        JobDetail job = JobBuilder.newJob(DeadlineJob.class)
                                  .withIdentity("early-job", "deadline")
                                  .usingJobData(DeadlineJob.DeadlineJobDataBinder.toJobData(converter, deadline, SCOPE))
                                  .build();
        AtomicReference<@Nullable JobExecutionException> jobFailure = new AtomicReference<>();
        scheduler.getListenerManager().addJobListener(new JobListenerSupport() {
            @Override
            public String getName() {
                return "jobFailureRecorder";
            }

            @Override
            public void jobWasExecuted(JobExecutionContext context, @Nullable JobExecutionException jobException) {
                jobFailure.compareAndSet(null, jobException);
            }
        });

        // when: the job fires while no QuartzDeadlineManager exists for the scheduler
        scheduler.scheduleJob(job, TriggerBuilder.newTrigger().startNow().build());

        // then
        await().atMost(FIRING_TIMEOUT).untilAsserted(() -> assertThat(jobFailure.get()).isNotNull());
        JobExecutionException failure = jobFailure.get();
        assertThat(failure).hasMessageContaining("fired before a QuartzDeadlineManager was created");
        assertThat(failure.refireImmediately()).isFalse();
        await().atMost(FIRING_TIMEOUT).until(() -> !scheduler.checkExists(job.getKey()));
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
