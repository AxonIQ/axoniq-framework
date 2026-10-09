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
