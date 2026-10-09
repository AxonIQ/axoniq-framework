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
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.junit.jupiter.api.*;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link QuartzDeadlineManager} beyond scheduling and firing deadlines.
 *
 * @author Jakob Hatzl
 */
class QuartzDeadlineManagerTest {

    @Nested
    class Shutdown {

        /**
         * Not every {@link Scheduler} implementation allows a repeated shutdown, so a repeated shutdown of the manager
         * must not reach it again.
         */
        @Test
        void shutsDownTheSchedulerOnlyOnce() throws SchedulerException {
            // given
            Properties properties = new Properties();
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "QuartzDeadlineManagerTest");
            properties.setProperty("org.quartz.threadPool.threadCount", "1");
            Scheduler scheduler = spy(new StdSchedulerFactory(properties).getScheduler());
            QuartzDeadlineManager deadlineManager = QuartzDeadlineManager.builder()
                                                                         .scheduler(scheduler)
                                                                         .scopeAwareProvider(scope -> Stream.empty())
                                                                         .unitOfWorkFactory(
                                                                                 UnitOfWorkTestUtils.SIMPLE_FACTORY
                                                                         )
                                                                         .converter(new JacksonConverter())
                                                                         .build();

            // when
            deadlineManager.shutdown();
            deadlineManager.shutdown();

            // then
            verify(scheduler, times(1)).shutdown(true);
        }
    }
}
