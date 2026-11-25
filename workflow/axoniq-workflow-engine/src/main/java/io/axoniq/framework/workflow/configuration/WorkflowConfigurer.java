/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.jspecify.annotations.Nullable;

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
    private @Nullable WorkflowSnapshotConfiguration snapshotConfiguration;

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
     * Applies the given {@code snapshotConfiguration} function to a new {@link WorkflowSnapshotConfiguration}, and
     * registers the result as the {@code WorkflowSnapshotConfiguration} component, which decides whether, and how,
     * the workflow engine snapshots its entities.
     * <p>
     * Unless snapshots are explicitly enabled or disabled, the workflow engine snapshots its entities when the
     * application can take snapshots: when it has a
     * {@link org.axonframework.eventsourcing.snapshot.store.SnapshotStore} component, a
     * {@link org.axonframework.conversion.GeneralConverter} component, and a
     * {@link org.axonframework.messaging.core.MessageTypeResolver} resolving the type of the entity. Explicitly
     * enabling snapshots requires all three, or the application fails to start. To take no snapshot at all, disable
     * them:
     * <pre>{@code
     * WorkflowConfigurer.create()
     *                   .snapshots(snapshots -> snapshots.enabled(false));
     * }</pre>
     * Every invocation starts from a new {@code WorkflowSnapshotConfiguration}, and replaces the configuration of an
     * earlier invocation.
     *
     * @param snapshotConfiguration a function customizing the {@link WorkflowSnapshotConfiguration}
     * @return the current instance of the {@code Configurer} for a fluent API
     */
    public WorkflowConfigurer snapshots(
            Function<WorkflowSnapshotConfiguration, WorkflowSnapshotConfiguration> snapshotConfiguration
    ) {
        var snapshots = requireNonNull(
                requireNonNull(snapshotConfiguration, "The snapshot configuration function must not be null.")
                        .apply(new WorkflowSnapshotConfiguration()),
                "The snapshot configuration function must not return null."
        );
        if (this.snapshotConfiguration == null) {
            delegate.componentRegistry(cr -> cr.registerComponent(
                    WorkflowSnapshotConfiguration.class, c -> requireNonNull(this.snapshotConfiguration)
            ));
        }
        this.snapshotConfiguration = snapshots;
        return this;
    }

    /**
     * Registers the {@link WorkflowIndex} of the workflow modules. Without one, which is the default, every segment
     * claim sources the running workflows and the open waits of all segments from the event store.
     * <p>
     * With an index, a claimed segment reads only its own rows, and a hibernated workflow stays out of memory across a
     * restart of the application. One index serves every workflow module.
     * <p>
     * Example usage:
     * <pre>{@code
     * WorkflowConfigurer.create()
     *                   .registerWorkflowIndex(config -> new InMemoryWorkflowIndex());
     * }</pre>
     *
     * @param indexBuilder builds the {@link WorkflowIndex} from the
     *                     {@link org.axonframework.common.configuration.Configuration}
     * @return this configurer, for a fluent API
     * @since 5.5.0
     */
    public WorkflowConfigurer registerWorkflowIndex(ComponentBuilder<WorkflowIndex> indexBuilder) {
        requireNonNull(indexBuilder, "The WorkflowIndex builder must not be null.");
        delegate.componentRegistry(cr -> cr.registerComponent(WorkflowIndex.class, indexBuilder));
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
