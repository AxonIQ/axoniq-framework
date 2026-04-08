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

package org.axonframework.springboot.autoconfig;

import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.config.ConfigurationScopeAwareProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.DeadlineManagerSpanFactory;
import org.axonframework.deadline.jobrunr.JobRunrDeadlineManager;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.scheduling.EventScheduler;
import org.axonframework.messaging.eventhandling.scheduling.jobrunr.JobRunrEventScheduler;
import org.axonframework.extension.springboot.autoconfig.AxonServerAutoConfiguration;
import org.axonframework.messaging.core.ScopeAwareProvider;
import org.axonframework.conversion.Serializer;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Auto configuration class for the deadline manager and event scheduler using JobRunr.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
@AutoConfiguration
@ConditionalOnBean(JobScheduler.class)
@AutoConfigureAfter(value = {AxonServerAutoConfiguration.class, AxonTracingAutoConfiguration.class},
        name = {"org.jobrunr.spring.autoconfigure.JobRunrAutoConfiguration"})
public class AxonJobRunrAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public EventScheduler eventScheduler(
            JobScheduler jobScheduler,
            @Qualifier("eventSerializer") Serializer serializer,
            TransactionManager transactionManager,
            EventBus eventBus) {
        return JobRunrEventScheduler.builder()
                                    .jobScheduler(jobScheduler)
                                    .serializer(serializer)
                                    .transactionManager(transactionManager)
                                    .eventBus(eventBus)
                                    .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadlineManager deadlineManager(
            JobScheduler jobScheduler,
            Configuration configuration,
            @Qualifier("eventSerializer") Serializer serializer,
            TransactionManager transactionManager,
            DeadlineManagerSpanFactory spanFactory
    ) {
        ScopeAwareProvider scopeAwareProvider = new ConfigurationScopeAwareProvider(configuration);
        return JobRunrDeadlineManager.builder()
                                     .jobScheduler(jobScheduler)
                                     .scopeAwareProvider(scopeAwareProvider)
                                     .serializer(serializer)
                                     .transactionManager(transactionManager)
                                     .spanFactory(spanFactory)
                                     .build();
    }
}
