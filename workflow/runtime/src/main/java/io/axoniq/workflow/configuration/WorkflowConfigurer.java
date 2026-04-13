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
package io.axoniq.workflow.configuration;

import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.messaging.core.configuration.MessagingConfigurationDefaults;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventBusConfigurationDefaults;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Configurer for the Workflow Engine.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowConfigurer implements ApplicationConfigurer {

    private final MessagingConfigurer delegate;

    private WorkflowConfigurer(MessagingConfigurer delegate) {
        this.delegate = requireNonNull(delegate, "The Messaging Configurer cannot be null.");
    }

    /**
     * Creates a new {@link WorkflowConfigurer} with the defaults required by the Workflow Engine.
     *
     * @return configurer.
     */
    public static WorkflowConfigurer create() {
        return enhance(MessagingConfigurer.create());
    }

    /**
     * Enhances the given {@link MessagingConfigurer} with the defaults required by the Workflow Engine.
     *
     * @param messagingConfigurer the messaging configurer to enhance.
     * @return enhanced configurer.
     */
    static WorkflowConfigurer enhance(MessagingConfigurer messagingConfigurer) {
        return new WorkflowConfigurer(messagingConfigurer)
                .componentRegistry(cr -> cr
                        .registerEnhancer(new EventBusConfigurationDefaults())
                        .registerEnhancer(new MessagingConfigurationDefaults())
                        .registerEnhancer(new EventSourcingConfigurationDefaults())
                        .registerEnhancer(new WorkflowConfigurerDefaults())
                        .registerEnhancer(new WorkflowEventProcessingRegistrationEnhancer(
                                "Workflow", null, null, true
                        ))
                );
    }

    @Override
    @Nonnull
    public WorkflowConfigurer componentRegistry(@Nonnull Consumer<ComponentRegistry> componentRegistrar) {
        delegate.componentRegistry(requireNonNull(componentRegistrar, "The configure task must no be null."));
        return this;
    }

    @Override
    @Nonnull
    public WorkflowConfigurer lifecycleRegistry(@Nonnull Consumer<LifecycleRegistry> lifecycleRegistrar) {
        delegate.lifecycleRegistry(requireNonNull(lifecycleRegistrar, "The lifecycle registrar must not be null."));
        return this;
    }

    @Override
    @Nonnull
    public AxonConfiguration start() {
        return delegate.start();
    }

    @Override
    @Nonnull
    public AxonConfiguration build() {
        return delegate.build();
    }
}
