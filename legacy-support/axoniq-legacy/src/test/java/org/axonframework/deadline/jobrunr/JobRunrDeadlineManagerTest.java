/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.jobrunr;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.InMemoryStorageProvider;
import org.junit.jupiter.api.*;

import java.util.stream.Stream;

import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link JobRunrDeadlineManager} beyond scheduling and firing deadlines.
 *
 * @author Jakob Hatzl
 */
class JobRunrDeadlineManagerTest {

    @Nested
    class Shutdown {

        /**
         * Shutting down the {@link JobScheduler} shuts down JobRunr as a whole, so a repeated shutdown of the manager
         * must not reach it again.
         */
        @Test
        void shutsDownTheJobSchedulerOnlyOnce() {
            // given
            JobScheduler jobScheduler = spy(new JobScheduler(new InMemoryStorageProvider()));
            doNothing().when(jobScheduler).shutdown();
            JobRunrDeadlineManager deadlineManager =
                    JobRunrDeadlineManager.builder()
                                          .jobScheduler(jobScheduler)
                                          .scopeAwareProvider(scope -> Stream.empty())
                                          .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                          .converter(new JacksonConverter())
                                          .build();

            // when
            deadlineManager.shutdown();
            deadlineManager.shutdown();

            // then
            verify(jobScheduler, times(1)).shutdown();
        }
    }
}
