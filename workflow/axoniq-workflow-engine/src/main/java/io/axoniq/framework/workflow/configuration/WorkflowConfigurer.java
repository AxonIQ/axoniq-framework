/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

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
 *     <li>{@link io.axoniq.framework.workflow.runtime.execution.WorkflowStore} - defaulting to an
 *     {@link io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowStore}</li>
 *     <li>{@link io.axoniq.framework.workflow.configuration.WorkflowMethodParameterResolverFactory}, so handler
 *     methods can inject the current workflow context, status, and other helpful information</li>
 *     <li>{@link io.axoniq.framework.workflow.index.api.WorkflowIndex} - defaulting to
 *     {@link io.axoniq.framework.workflow.index.api.WorkflowIndex#none()}, which keeps no index, see
 *     {@link #registerWorkflowIndex(ComponentBuilder)}</li>
 * </ul>
 * Every {@link io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry},
 * {@link io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository},
 * {@link io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService},
 * {@link io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository}, and
 * {@link io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager} is, by contrast, <em>not</em> among these
 * app-wide defaults. Each {@link WorkflowModule} registers its own instance of these, named after itself, so several
 * {@code WorkflowModules} in one application never share any of them.
 * <p>
 * To replace or decorate any of the remaining app-wide defaults above, use their respective type as the identifier.
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
     * Creates a {@code WorkflowConfigurer} that enhances an existing {@code EventSourcingConfigurer}.
     * <p>
     * This method is useful when applying multiple specialized Configurers to configure a single application.
     *
     * @param eventSourcingConfigurer the {@code EventSourcingConfigurer} to enhance with configuration of messaging
     *                                components
     * @return the current instance of the {@code Configurer} for a fluent API
     * @see #create()
     */
    public static WorkflowConfigurer enhance(EventSourcingConfigurer eventSourcingConfigurer) {
        return new WorkflowConfigurer(eventSourcingConfigurer)
                .componentRegistry(cr -> cr.registerEnhancer(new WorkflowConfigurationDefaults()));
    }

    /**
     * Creates a new configurer with the defaults required by the Workflow Engine.
     *
     * @return configurer
     */
    public static WorkflowConfigurer create() {
        return enhance(EventSourcingConfigurer.create());
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
     * Registers the given {@link WorkflowIndex} builder, replacing the default {@link WorkflowIndex#none()}, which keeps
     * no index, so every claim of a segment reads the running workflow instances from the event store.
     * <p>
     * Every workflow module of the application keeps its index of running workflow instances in this one
     * {@code WorkflowIndex}, each under its own name. A durable index shared by every node, such as the
     * {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex}, lets a restarted application, or another node
     * claiming a segment, restore the running workflow instances of the segment without reading every workflow
     * lifecycle event:
     * <pre>{@code
     * WorkflowConfigurer.create()
     *                   .registerWorkflowIndex(config -> JpaWorkflowIndex.builder()
     *                                                    .entityManagerFactory(entityManagerFactory)
     *                                                    .build());
     * }</pre>
     * Every checkpoint of a segment writes the changes of the index first, within the transaction the token is stored
     * in when the index can join it. When the index cannot be written, the checkpoint fails, and the event processor
     * releases the segment and claims it again later. With an index, the event processor of a workflow module must let
     * the engine decide when to store its tokens: a module whose processor has ordinary event handling components,
     * which do not checkpoint on their own, or another way of storing its tokens, fails to start.
     * <p>
     * The tokens of that event processor must survive a restart whenever the index does: a module that keeps an index
     * other than {@link WorkflowIndex#none()} or an
     * {@link io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex}, while its processor keeps its tokens
     * in an
     * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore},
     * fails to start. When you delete or reset the tokens of that processor, empty the index as well.
     *
     * @param workflowIndexBuilder the builder constructing the {@link WorkflowIndex}
     * @return the current instance of the {@code Configurer} for a fluent API
     */
    public WorkflowConfigurer registerWorkflowIndex(ComponentBuilder<WorkflowIndex> workflowIndexBuilder) {
        requireNonNull(workflowIndexBuilder, "The workflow index builder must not be null.");
        delegate.componentRegistry(cr -> cr.registerComponent(WorkflowIndex.class, workflowIndexBuilder));
        return this;
    }

    /**
     * Applies the given {@code snapshotConfiguration} function to the {@link WorkflowSnapshotConfiguration} of the
     * workflow engine.
     * <p>
     * Snapshots are disabled by default. Enabling them requires a
     * {@link org.axonframework.eventsourcing.snapshot.store.SnapshotStore} component.
     *
     * @param snapshotConfiguration a function customizing the {@link WorkflowSnapshotConfiguration}
     * @return the current instance of the {@code Configurer} for a fluent API
     */
    public WorkflowConfigurer snapshots(
            Function<WorkflowSnapshotConfiguration, WorkflowSnapshotConfiguration> snapshotConfiguration
    ) {
        var snapshots = requireNonNull(snapshotConfiguration, "The snapshot configuration function must not be null.")
                .apply(new WorkflowSnapshotConfiguration());
        if (snapshots.enabled()) {
            delegate.componentRegistry(cr -> cr
                    .registerComponent(
                            SnapshotPolicy.class,
                            WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY,
                            c -> snapshots.runningWorkflows()
                    )
                    .registerComponent(
                            SnapshotPolicy.class,
                            WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY,
                            c -> snapshots.workflowState()
                    ));
        }
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
