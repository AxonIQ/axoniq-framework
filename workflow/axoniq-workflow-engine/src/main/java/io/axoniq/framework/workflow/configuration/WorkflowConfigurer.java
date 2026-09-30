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

import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;

import java.util.Objects;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * The workflow {@link ApplicationConfigurer} of Axon Framework's configuration API.
 * <p>
 * Provides register operations for {@link #registerWorkflowModule(WorkflowModule) the workflow module} infrastructure
 * components.
 * <p>
 * Through {@link io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults}, this configurer registers
 * the following defaults, each only if the application has not already registered one of its own:
 * <ul>
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.payload.PayloadReducerRegistry}</li>
 *     <li>{@link io.axoniq.framework.workflow.runtime.util.FutureResolver} - discovered through the
 *     {@code ServiceLoader}, falling back to a
 *     {@link io.axoniq.framework.workflow.runtime.util.DefaultTimeoutFutureResolver}</li>
 *     <li>{@link io.axoniq.framework.workflow.dsl.api.EventNameCustomizer} - defaulting to a
 *     {@link io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer}</li>
 *     <li>{@link java.time.Clock} - defaulting to {@link org.axonframework.common.ClockUtils#get()}</li>
 *     <li>A decorator wrapping the registered {@link org.axonframework.eventsourcing.eventstore.TagResolver} with a
 *     {@link io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver}</li>
 *     <li>A declarative event-sourced entity module for
 *     {@link io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState}</li>
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver} - defaulting to a
 *     {@link io.axoniq.framework.workflow.runtime.execution.DefaultExecuteStepActionResolver}</li>
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler} - defaulting to a
 *     {@link io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler}</li>
 *     <li>A declarative event-sourced entity module for
 *     {@link io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows}</li>
 *     <li>A named {@link java.util.concurrent.ExecutorService} for workflow-body work and workflow-event
 *     publication (see {@link io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults#WORKFLOW_ENGINE_EXECUTOR}),
 *     defaulting to a virtual-thread-per-task executor</li>
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService}</li>
 *     <li>{@link io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository} - defaulting to an
 *     {@link io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository}</li>
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.WorkflowStore} - defaulting to an
 *     {@link io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowStore}</li>
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.WorkflowStateParameterResolverFactory}, so handler
 *     methods can inject the current workflow state</li>
 * </ul>
 * Every {@link io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry},
 * {@link io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository}, and
 * {@link io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager} is, by contrast, <em>not</em> among these
 * app-wide defaults: each {@link WorkflowModule} registers its own instance of these three, named after itself, so
 * several {@code WorkflowModule}s in one application never share - or collide over - any of them.
 * <p>
 * To replace or decorate any of the app-wide defaults above, use their respective type as the identifier. For example,
 * to replace the default {@code MutableWorkflowHistoryRepository}, do
 * <pre><code>
 *     configurer.componentRegistry(cr ->
 *                  cr.registerComponent(MutableWorkflowHistoryRepository.class, c -> new CustomWorkflowHistoryRepository()))
 * </code></pre>
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
     * Creates a new configurer with the defaults required by the Workflow Engine.
     *
     * @return configurer.
     */
    public static WorkflowConfigurer create() {
        return new WorkflowConfigurer(EventSourcingConfigurer.create());
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
