/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager;

/**
 * A {@link ConfigurationEnhancer} that ties every {@link DeadlineManager} of the configuration to the application's
 * lifecycle.
 * <p>
 * It registers a decorator for the {@code DeadlineManager} type that leaves the manager as it is, and calls
 * {@link DeadlineManager#shutdown()} in the {@link Phase#INBOUND_EVENT_CONNECTORS} shutdown phase. Deadlines thereby
 * stop firing before the components that handle them shut down. In the same start phase, it calls
 * {@link DbSchedulerDeadlineManager#start()} on a {@link DbSchedulerDeadlineManager}, which starts its scheduler unless
 * {@link DbSchedulerDeadlineManager.Builder#startScheduler(boolean) startScheduler} leaves that to the application. The
 * decorator applies to every component that is a {@code DeadlineManager}, under whichever type it is registered. It
 * runs before every other decorator, so it sees the manager itself rather than a wrapper of it.
 * <p>
 * Applications therefore do not start or shut down a {@code DeadlineManager} of the configuration themselves. A
 * {@code DeadlineManager} may still be shut down more than once, for example when Spring calls the
 * {@code shutdown()} method it infers as the destroy method of a {@code @Bean}, so its implementations allow repeated
 * calls.
 * <p>
 * The enhancer is registered through the {@link java.util.ServiceLoader} mechanism, and can be disabled with
 * {@link ComponentRegistry#disableEnhancer(Class)} by an application that starts and shuts down its deadline managers
 * itself.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@RegistrationScope("Register the decorator once at the root; the DecoratorDefinition is copied down and reaches "
        + "module-built deadline managers on its own.")
public class DeadlineManagerLifecycleConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(
                DecoratorDefinition.forType(DeadlineManager.class)
                                   .with((config, name, deadlineManager) -> deadlineManager)
                                   .order(Integer.MIN_VALUE)
                                   .onStart(Phase.INBOUND_EVENT_CONNECTORS, deadlineManager -> {
                                       if (deadlineManager instanceof DbSchedulerDeadlineManager dbScheduler) {
                                           dbScheduler.start();
                                       }
                                   })
                                   .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, DeadlineManager::shutdown)
        );
    }
}
