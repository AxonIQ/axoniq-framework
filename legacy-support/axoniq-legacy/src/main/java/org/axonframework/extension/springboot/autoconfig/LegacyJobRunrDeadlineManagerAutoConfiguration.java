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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.jobrunr.JobRunrDeadlineManager;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jobrunr.scheduling.JobScheduler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures a {@link JobRunrDeadlineManager} when the application has a JobRunr {@link JobScheduler}, as Axon
 * Framework 4's Spring Boot auto-configuration did.
 * <p>
 * The manager converts the deadlines it stores with the {@link EventConverter}, the counterpart of the event serializer
 * Axon Framework 4 wired, so that deadlines an Axon Framework 4 application scheduled keep firing. A fired deadline
 * runs in a unit of work from the configuration's default {@link UnitOfWorkFactory}, and is delivered through the
 * configuration's {@link ScopeAwareProvider}, with which the Saga managers register themselves. Define a {@link
 * DeadlineManager} bean to configure the manager differently, for example with another converter. This is needed, for
 * example, when running a {@link DeadlineManager} in an upgrade scenario with an {@code XStream}-based serializer
 * in Axon Framework 4.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@AutoConfiguration(afterName = {
        "org.jobrunr.spring.autoconfigure.JobRunrAutoConfiguration"
})
@ConditionalOnClass({JobScheduler.class, JobRunrDeadlineManager.class})
@ConditionalOnBean(JobScheduler.class)
public class LegacyJobRunrDeadlineManagerAutoConfiguration {

    /**
     * Creates the {@link JobRunrDeadlineManager}, unless the application defines its own {@link DeadlineManager}.
     *
     * @param jobScheduler       the JobRunr scheduler storing and firing the deadlines
     * @param scopeAwareProvider the configuration's provider of the components a fired deadline is delivered to
     * @param configuration      the Axon configuration, providing the default {@link UnitOfWorkFactory}, the factory
     *                           of the unit of work a fired deadline runs in
     * @param eventConverter     the converter of the stored deadlines
     * @return the JobRunr deadline manager
     */
    @Bean
    @ConditionalOnMissingBean(DeadlineManager.class)
    public JobRunrDeadlineManager deadlineManager(JobScheduler jobScheduler,
                                                  ScopeAwareProvider scopeAwareProvider,
                                                  Configuration configuration,
                                                  EventConverter eventConverter) {
        return JobRunrDeadlineManager.builder()
                                     .jobScheduler(jobScheduler)
                                     .scopeAwareProvider(scopeAwareProvider)
                                     .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
                                     .converter(eventConverter)
                                     .build();
    }
}
