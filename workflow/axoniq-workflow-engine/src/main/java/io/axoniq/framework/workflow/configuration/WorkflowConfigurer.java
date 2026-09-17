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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;

import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;

import java.util.Objects;
import java.util.function.Consumer;

import static io.axoniq.framework.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME;
import static java.util.Objects.requireNonNull;

/**
 * The workflow {@link ApplicationConfigurer} of Axon Framework's configuration API.
 * <p>
 * Provides register operations for {@link #registerWorkflowModule(WorkflowModule) the workflow module} infrastructure
 * components.
 * <p>
 * This configurer registers the following defaults:
 * <ul>
 *     <li>Registers a {@link io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer} for class {@link io.axoniq.framework.workflow.dsl.api.EventNameCustomizer}</li>
 *     <li>Registers a {@link io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository} </li>
 *     <li>Registers a {@link io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository}</li>
 *     <li>Registers a {@link io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry}</li>
 *     <li>Registers a {@link io.axoniq.framework.workflow.runtime.execution.WorkflowEngine}</li>
 *     <li>Registers a {@link io.axoniq.framework.workflow.history.inmemory.WorkflowHistoryProjector}</li>
 * </ul>
 * To replace or decorate any of these defaults, use their respective interfaces as the identifier. For example, to
 * adjust the {@code InMemoryWorkflowHistoryRepository}, do
 * <pre><code>
 *     configurer.componentRegistry(cr ->
 *                  cr.registerComponent(InMemoryWorkflowHistoryRepository.class, c -> new CustomWorkflowHistoryRepository()))
 * </code></pre>
 * to replace it.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowConfigurer implements ApplicationConfigurer {

    private final EventSourcingConfigurer delegate;

    private WorkflowConfigurer(EventSourcingConfigurer delegate) {
        this.delegate = requireNonNull(delegate, "The Event Sourcing Configurer cannot be null.");
    }

    /**
     * Creates a new {@link WorkflowConfigurer} with the defaults required by the Workflow Engine.
     *
     * @return configurer.
     */
    public static WorkflowConfigurer create() {
        return enhance(EventSourcingConfigurer.create());
    }

    /**
     * Enhances the given {@link EventSourcingConfigurer} with the defaults required by the Workflow Engine.
     *
     * @param eventSourcingConfigurer the event sourcing configurer to enhance.
     * @return enhanced configurer.
     */
    static WorkflowConfigurer enhance(EventSourcingConfigurer eventSourcingConfigurer) {
        return new WorkflowConfigurer(eventSourcingConfigurer)
                .componentRegistry(cr -> cr
                        .registerEnhancer(new WorkflowEventProcessingRegistrationEnhancer(
                                                  DEFAULT_MODULE_NAME,
                                                  null,
                                                  null,
                                                  true
                                          )
                        )
                );
    }

    /**
     * Registers a workflow module.
     *
     * @param workflowModule workflow module to register.
     * @return this configurer.
     */
    public WorkflowConfigurer registerWorkflowModule(WorkflowModule<?> workflowModule) {
        Objects.requireNonNull(workflowModule, "Workflow module must not be null");
        delegate.componentRegistry(cr -> cr.registerModule(workflowModule));
        return this;
    }


    /**
     * Delegates the given {@code configurerTask} to the {@link EventSourcingConfigurer} this
     * {@code EventSourcingConfigurer} delegates.
     * <p>
     * Use this operation to invoke registration methods that only exist on the {@code EventSourcingConfigurer}.
     *
     * @param configurerTask Lambda consuming the delegate {@link EventSourcingConfigurer}.
     * @return The current instance of the {@code Configurer} for a fluent API.
     */
    public WorkflowConfigurer eventSourcing(Consumer<EventSourcingConfigurer> configurerTask) {
        requireNonNull(configurerTask, "The configure task must no be null.").accept(delegate);
        return this;
    }

    @Override
    public WorkflowConfigurer componentRegistry(Consumer<ComponentRegistry> componentRegistrar) {
        delegate.componentRegistry(requireNonNull(componentRegistrar, "The component registrar must no be null."));
        return this;
    }

    @Override
    public WorkflowConfigurer lifecycleRegistry(Consumer<LifecycleRegistry> lifecycleRegistrar) {
        delegate.lifecycleRegistry(requireNonNull(lifecycleRegistrar, "The lifecycle registrar must not be null."));
        return this;
    }

    @Override
    public AxonConfiguration start() {
        return delegate.start();
    }

    @Override
    public AxonConfiguration build() {
        return delegate.build();
    }
}
