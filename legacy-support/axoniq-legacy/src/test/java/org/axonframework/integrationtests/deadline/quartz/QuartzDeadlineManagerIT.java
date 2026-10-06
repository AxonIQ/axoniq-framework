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

package org.axonframework.integrationtests.deadline.quartz;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.AxonNonTransientException;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.quartz.DeadlineJob;
import org.axonframework.deadline.quartz.QuartzDeadlineManager;
import org.axonframework.integrationtests.deadline.AbstractDeadlineManagerTestSuite;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the {@link AbstractDeadlineManagerTestSuite} against the {@link QuartzDeadlineManager}, on an in-memory Quartz
 * {@link Scheduler}.
 */
class QuartzDeadlineManagerIT extends AbstractDeadlineManagerTestSuite {

    private Scheduler scheduler;

    @Override
    protected AbstractDeadlineManager buildDeadlineManager(ScopeAwareProvider scopeAwareProvider,
                                                           UnitOfWorkFactory unitOfWorkFactory) {
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            QuartzDeadlineManager manager = QuartzDeadlineManager.builder()
                                                                 .scheduler(scheduler)
                                                                 .scopeAwareProvider(scopeAwareProvider)
                                                                 .unitOfWorkFactory(unitOfWorkFactory)
                                                                 .converter(new JacksonConverter())
                                                                 .build();
            scheduler.start();
            return manager;
        } catch (SchedulerException e) {
            throw new AxonConfigurationException("Unable to configure quartz scheduler", e);
        }
    }

    @Test
    void shutdownShutsTheSchedulerDown() throws SchedulerException {
        // when
        deadlineManager.shutdown();

        // then
        assertThat(scheduler.isShutdown()).isTrue();
    }

    @Test
    void aTransientDeliveryFailureIsRefiredImmediately() {
        // given
        scopeAware.failWith(new IllegalStateException("transient failure"));

        // when
        deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);
        await().atMost(FIRING_TIMEOUT).until(() -> !scopeAware.attempts().isEmpty());
        scopeAware.failWith(null);

        // then
        assertThat(awaitSingleDelivery().message().payload()).isEqualTo("payload");
        assertThat(scopeAware.attempts()).hasSizeGreaterThan(1);
    }

    @Test
    void aNonTransientDeliveryFailureDeletesTheJobWithoutRefiring() {
        // given
        scopeAware.failWith(new NonTransientFailure());

        // when
        deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

        // then
        await().atMost(FIRING_TIMEOUT).until(() -> !scopeAware.attempts().isEmpty());
        await().during(NOT_FIRING_PERIOD).atMost(NOT_FIRING_PERIOD.plusSeconds(1))
               .until(() -> scopeAware.attempts().size() == 1);
        await().atMost(FIRING_TIMEOUT).until(() -> scheduler.getJobKeys(GroupMatcher.jobGroupEquals(DEADLINE_NAME))
                                                            .isEmpty());
    }

    @Test
    void aJobWhoseDataCannotBeReadIsDeletedAfterOneAttempt() throws SchedulerException {
        // given
        JobDetail unreadableJob = JobBuilder.newJob(DeadlineJob.class)
                                            .withIdentity("deadline-unreadable", DEADLINE_NAME)
                                            .usingJobData("serializedDeadlineMessage", "axon 3.3 layout")
                                            .build();

        // when
        scheduler.scheduleJob(unreadableJob, TriggerBuilder.newTrigger().startNow().build());

        // then
        await().atMost(FIRING_TIMEOUT).until(() -> !scheduler.checkExists(unreadableJob.getKey()));
        assertThat(scopeAware.attempts()).isEmpty();
    }

    private static final class NonTransientFailure extends AxonNonTransientException {

        private NonTransientFailure() {
            super("non-transient failure");
        }
    }
}
