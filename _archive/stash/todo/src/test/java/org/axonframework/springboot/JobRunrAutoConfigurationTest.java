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

package org.axonframework.springboot;

import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.jobrunr.JobRunrDeadlineManager;
import org.axonframework.messaging.eventhandling.scheduling.EventScheduler;
import org.axonframework.messaging.eventhandling.scheduling.jobrunr.JobRunrEventScheduler;
import org.jobrunr.scheduling.JobScheduler;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Disabled("TODO #3104 & #3065")
class JobRunrAutoConfigurationTest {

    @Test
    void eventSchedulerAndDeadlineManagercreated() {
        new ApplicationContextRunner()
                .withPropertyValues("axon.axonserver.enabled=false")
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    EventScheduler eventScheduler = context.getBean(EventScheduler.class);
                    assertNotNull(eventScheduler);
                    assertTrue(eventScheduler instanceof JobRunrEventScheduler);
                    DeadlineManager deadlineManager = context.getBean(DeadlineManager.class);
                    assertNotNull(deadlineManager);
                    assertTrue(deadlineManager instanceof JobRunrDeadlineManager);
                });
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class DefaultContext {
        @Bean
        public JobScheduler jobScheduler(){
            return mock(JobScheduler.class);
        }
    }
}
