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

package org.axonframework.integrationtests.deadline.jobrunr;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.jobrunr.JobRunrDeadlineManager;
import org.axonframework.integrationtests.deadline.AbstractDeadlineManagerTestSuite;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.modelling.command.AggregateScopeDescriptor;
import org.jobrunr.configuration.JobRunr;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobBuilder;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.InMemoryStorageProvider;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.jobrunr.server.BackgroundJobServerConfiguration.usingStandardBackgroundJobServerConfiguration;

/**
 * Runs the {@link AbstractDeadlineManagerTestSuite} against the {@link JobRunrDeadlineManager}, on JobRunr's in-memory
 * storage.
 */
class JobRunrDeadlineManagerIT extends AbstractDeadlineManagerTestSuite {

    private StorageProvider storageProvider;
    private BackgroundJobServer backgroundJobServer;

    @Override
    protected AbstractDeadlineManager buildDeadlineManager(ScopeAwareProvider scopeAwareProvider,
                                                           UnitOfWorkFactory unitOfWorkFactory) {
        // JobRunr is configured statically, so the server of a manager built earlier in the test has to stop first.
        stopBackgroundJobServer();
        storageProvider = new InMemoryStorageProvider();
        JobRunrDeadlineManager manager = JobRunrDeadlineManager.builder()
                                                               .jobScheduler(new JobScheduler(storageProvider))
                                                               .scopeAwareProvider(scopeAwareProvider)
                                                               .unitOfWorkFactory(unitOfWorkFactory)
                                                               .converter(new JacksonConverter())
                                                               .build();
        JobRunr.configure()
               .useJobActivator(new SimpleActivator<>(manager))
               .useStorageProvider(storageProvider)
               .useBackgroundJobServer(
                       usingStandardBackgroundJobServerConfiguration().andPollInterval(Duration.ofMillis(200))
               )
               .initialize();
        backgroundJobServer = JobRunr.getBackgroundJobServer();
        return manager;
    }

    @AfterEach
    void stopBackgroundJobServer() {
        if (backgroundJobServer != null) {
            backgroundJobServer.stop();
            backgroundJobServer = null;
        }
    }

    @Override
    protected boolean supportsCancellingByNameAndScope() {
        return false;
    }

    @Test
    void cancellingAnAlreadyCancelledScheduleDoesNotThrow() {
        // given
        String scheduleId = deadlineManager.schedule(Duration.ofMinutes(15), DEADLINE_NAME, null,
                                                     new AggregateScopeDescriptor("aggregateType", "aggregateId"));
        deadlineManager.cancelSchedule(DEADLINE_NAME, scheduleId);

        // when / then
        assertThatCode(() -> deadlineManager.cancelSchedule(DEADLINE_NAME, scheduleId)).doesNotThrowAnyException();
    }

    @Test
    void aFailingDeliveryIsRetried() {
        // given
        scopeAware.failWith(new IllegalStateException("delivery failure"));

        // when
        deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);
        await().atMost(FIRING_TIMEOUT).until(() -> !scopeAware.attempts().isEmpty());
        scopeAware.failWith(null);

        // then
        await().atMost(Duration.ofSeconds(30)).until(() -> !scopeAware.deliveries().isEmpty());
        assertThat(scopeAware.attempts()).hasSizeGreaterThan(1);
    }

    @Test
    void aJobWhoseDetailsCannotBeReadIsKeptForRetrying() {
        // given
        JobRunrDeadlineManager manager = (JobRunrDeadlineManager) deadlineManager;

        // when
        JobId jobId = new JobScheduler(storageProvider).create(
                JobBuilder.aJob()
                          .withName(DEADLINE_NAME)
                          .withDetails(() -> manager.execute("not the details of a deadline", "deadlineId"))
                          .scheduleAt(Instant.now())
        );

        // then
        await().atMost(FIRING_TIMEOUT).untilAsserted(() -> {
            Job job = storageProvider.getJobById(jobId);
            assertThat(job.getJobStates()).anySatisfy(state -> assertThat(state.getName())
                    .isEqualTo(StateName.FAILED));
            assertThat(job.getState()).isEqualTo(StateName.SCHEDULED);
        });
        assertThat(scopeAware.attempts()).isEmpty();
    }
}
