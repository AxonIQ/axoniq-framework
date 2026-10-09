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

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.Task;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.dbscheduler.DbSchedulerBinaryDeadlineDetails;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures a {@link DbSchedulerDeadlineManager} when the application has a db-scheduler {@link Scheduler}, as
 * Axon Framework 4's Spring Boot auto-configuration did.
 * <p>
 * The manager stores deadlines as {@link DbSchedulerBinaryDeadlineDetails}, and this configuration provides the
 * {@code deadlineDetailsTask} that executes them, which db-scheduler's own Spring Boot auto-configuration registers
 * with the scheduler. The manager converts the deadlines with the {@link EventConverter}, the counterpart of the event
 * serializer Axon Framework 4 wired, so that deadlines an Axon Framework 4 application scheduled keep firing. A fired
 * deadline runs in a unit of work from the configuration's default {@link UnitOfWorkFactory}, and is delivered through
 * the application's {@link ScopeAwareProvider} bean. Without that bean, no manager is configured. The scheduler's
 * lifecycle stays with db-scheduler's auto-configuration. Define a {@link DeadlineManager} bean to configure the
 * manager differently, for example with another converter. This is needed, for example, when running a
 * {@link DeadlineManager} in an upgrade scenario with an {@code XStream}-based serializer in Axon Framework 4.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@AutoConfiguration(afterName = {
        "com.github.kagkarlsson.scheduler.boot.autoconfigure.DbSchedulerAutoConfiguration",
        "org.axonframework.extension.springboot.autoconfig.LegacySagaAutoConfiguration"
})
@ConditionalOnClass({Scheduler.class, DbSchedulerDeadlineManager.class})
public class LegacyDbSchedulerDeadlineManagerAutoConfiguration {

    /**
     * Creates the task executing the deadlines the {@link DbSchedulerDeadlineManager} stores, unless the application
     * defines a bean named {@code deadlineDetailsTask}. The task resolves the manager when a deadline fires.
     *
     * @param context the application context to resolve the {@link DbSchedulerDeadlineManager} from
     * @return the task executing deadlines
     */
    @Bean(name = "deadlineDetailsTask")
    @ConditionalOnMissingBean(name = "deadlineDetailsTask")
    public Task<DbSchedulerBinaryDeadlineDetails> dbSchedulerDeadlineDetailsTask(ApplicationContext context) {
        return DbSchedulerDeadlineManager.binaryTask(() -> context.getBean(DbSchedulerDeadlineManager.class));
    }

    /**
     * Creates the {@link DbSchedulerDeadlineManager}, unless the application defines its own {@link DeadlineManager}.
     * The manager neither starts nor stops the scheduler, as db-scheduler's Spring Boot auto-configuration does.
     *
     * @param scheduler          the db-scheduler scheduler storing and firing the deadlines
     * @param scopeAwareProvider the provider of the components a fired deadline is delivered to
     * @param configuration      the Axon configuration, providing the default {@link UnitOfWorkFactory}, the factory
     *                           of the unit of work a fired deadline runs in
     * @param eventConverter     the converter of the stored deadlines
     * @return the db-scheduler deadline manager
     */
    @Bean
    @ConditionalOnBean({Scheduler.class, ScopeAwareProvider.class})
    @ConditionalOnMissingBean(DeadlineManager.class)
    public DbSchedulerDeadlineManager deadlineManager(Scheduler scheduler,
                                                      ScopeAwareProvider scopeAwareProvider,
                                                      Configuration configuration,
                                                      EventConverter eventConverter) {
        return DbSchedulerDeadlineManager.builder()
                                         .scheduler(scheduler)
                                         .scopeAwareProvider(scopeAwareProvider)
                                         .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
                                         .converter(eventConverter)
                                         .startScheduler(false)
                                         .stopScheduler(false)
                                         .build();
    }
}
