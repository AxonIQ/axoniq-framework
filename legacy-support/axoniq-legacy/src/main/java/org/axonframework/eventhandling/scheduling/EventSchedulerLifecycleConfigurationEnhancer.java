/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.eventhandling.scheduling;

import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.lifecycle.Phase;

/**
 * A {@link ConfigurationEnhancer} that ties every {@link EventScheduler} of the configuration to the application's
 * lifecycle.
 * <p>
 * It registers a decorator for the {@code EventScheduler} type that leaves the scheduler as it is, and calls
 * {@link EventScheduler#shutdown()} in the {@link Phase#INBOUND_EVENT_CONNECTORS} shutdown phase, as Axon Framework 4
 * did. Scheduled events thereby stop firing before the components that handle them shut down. The decorator applies to
 * every component that is an {@code EventScheduler}, under whichever type it is registered. It runs before every other
 * decorator, so it sees the scheduler itself rather than a wrapper of it.
 * <p>
 * Applications therefore do not shut down an {@code EventScheduler} of the configuration themselves. An
 * {@code EventScheduler} may still be shut down more than once, for example when Spring calls the {@code shutdown()}
 * method it infers as the destroy method of a {@code @Bean}, so its implementations allow repeated calls.
 * <p>
 * The enhancer is registered through the {@link java.util.ServiceLoader} mechanism, and can be disabled with
 * {@link ComponentRegistry#disableEnhancer(Class)} by an application that shuts down its event schedulers itself.
 *
 * @author Jakob Hatzl
 * @since 5.5.0
 */
@RegistrationScope("Register the decorator once at the root; the DecoratorDefinition is copied down and reaches "
        + "module-built event schedulers on its own.")
public class EventSchedulerLifecycleConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(
                DecoratorDefinition.forType(EventScheduler.class)
                                   .with((config, name, eventScheduler) -> eventScheduler)
                                   .order(Integer.MIN_VALUE)
                                   .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, EventScheduler::shutdown)
        );
    }
}
