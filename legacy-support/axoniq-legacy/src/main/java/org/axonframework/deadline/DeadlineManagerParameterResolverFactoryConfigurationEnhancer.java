/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;

/**
 * A {@link ConfigurationEnhancer} that makes a {@link DeadlineManager} handler parameter defer its calls to the
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} of the handler.
 * <p>
 * Axon Framework 4 deferred every deadline call made while a unit of work was active to that unit of work's
 * prepare-commit phase, so a command that failed after scheduling a deadline never stored it. Axon Framework 5 has no
 * ambient unit of work, and the command handler of an entity runs in no {@link org.axonframework.messaging.Scope}
 * that could carry its context. This enhancer registers a
 * {@link org.axonframework.messaging.core.annotation.ParameterResolverFactory} that resolves a parameter declared as
 * {@link DeadlineManager} to a view of the configured {@link AbstractDeadlineManager}, bound to the context of the
 * handler. Calls made through it are deferred to that context, and its dispatch interceptors receive it, as for a
 * Saga handler.
 * <p>
 * The parameter is resolved to the configured component itself when it is declared as an implementation type, when
 * no {@link DeadlineManager} is configured, or when the configured one does not extend
 * {@link AbstractDeadlineManager}, such as the {@code org.axonframework.test.deadline.StubDeadlineManager} of the saga
 * test fixture. A {@link DeadlineManager} reached in any other way than as a handler parameter, for example as a field
 * of a collaborator, is not bound to a context, so its calls outside a Saga handler run immediately. The bound view is
 * not the configured instance, so a handler that casts the parameter to its implementation or compares it by identity
 * should declare the implementation type instead, or disable this enhancer.
 * <p>
 * The enhancer is discovered through the {@link java.util.ServiceLoader}, so an application gets this behaviour by
 * having {@code axoniq-legacy} on its classpath. An application can opt out with
 * {@link ComponentRegistry#disableEnhancer(Class)}:
 * <pre>{@code
 * MessagingConfigurer.create()
 *                    .componentRegistry(registry -> registry.disableEnhancer(
 *                            DeadlineManagerParameterResolverFactoryConfigurationEnhancer.class
 *                    ));
 * }</pre>
 *
 * @author Mateusz Nowak
 * @since 5.4.0
 */
public class DeadlineManagerParameterResolverFactoryConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                registry, DeadlineManagerParameterResolverFactory::new
        );
    }
}
