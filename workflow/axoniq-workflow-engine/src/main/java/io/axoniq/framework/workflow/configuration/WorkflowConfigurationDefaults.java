/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.DefaultExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.framework.workflow.runtime.util.DefaultTimeoutFutureResolver;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import org.axonframework.common.ClockUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.eventsourcing.eventstore.MultiTagResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;

import java.time.Clock;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Defaults for workflow configuration.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public class WorkflowConfigurationDefaults implements ConfigurationEnhancer {

    /**
     * Name of the dedicated executor service component used for workflow-body work and workflow-event publication.
     * <p>
     * The default uses a virtual thread per task. A bounded future-resolution wait therefore blocks only its workflow
     * task, not an event processor or another shared executor. Applications that replace this component should retain
     * that isolation or size their executor for the configured resolver timeout.
     */
    public static final String WORKFLOW_ENGINE_EXECUTOR = "WorkflowEngineExecutor";
    /**
     * The name under which a {@link SnapshotPolicy} component enables snapshots of the running-workflow list, as it
     * did before 5.5.0.
     *
     * @deprecated Configure snapshots through {@link WorkflowConfigurer#snapshots}, or register a
     * {@link WorkflowSnapshotConfiguration} component. A component registered under this name still snapshots the
     * running-workflow list with its policy, and logs a warning, as long as the {@code WorkflowSnapshotConfiguration}
     * neither enables nor disables snapshots. Its policy then takes precedence over the running-workflow policy of the
     * {@code WorkflowSnapshotConfiguration}, including one configured through {@link WorkflowConfigurer#snapshots} or
     * the Spring Boot {@code axon.workflow.snapshots.running-workflows.after-events} property. Once the
     * {@code WorkflowSnapshotConfiguration} enables or disables snapshots, the component is ignored, and a warning is
     * logged.
     */
    @Deprecated(since = "5.5.0")
    public static final String RUNNING_WORKFLOWS_SNAPSHOT_POLICY = "RunningWorkflowsSnapshotPolicy";
    /**
     * The name under which a {@link SnapshotPolicy} component enables snapshots of the workflow state, as it did
     * before 5.5.0.
     *
     * @deprecated Configure snapshots through {@link WorkflowConfigurer#snapshots}, or register a
     * {@link WorkflowSnapshotConfiguration} component. A component registered under this name still snapshots the
     * workflow state with its policy, and logs a warning, as long as the {@code WorkflowSnapshotConfiguration} neither
     * enables nor disables snapshots. Its policy then takes precedence over the workflow-state policy of the
     * {@code WorkflowSnapshotConfiguration}, including one configured through {@link WorkflowConfigurer#snapshots} or
     * the Spring Boot {@code axon.workflow.snapshots.workflow-state.after-events} property. Once the
     * {@code WorkflowSnapshotConfiguration} enables or disables snapshots, the component is ignored, and a warning is
     * logged.
     */
    @Deprecated(since = "5.5.0")
    public static final String WORKFLOW_STATE_SNAPSHOT_POLICY = "WorkflowStateSnapshotPolicy";
    /**
     * Order for this enhancer.
     * <p>
     * Enhancer math: register the tag-resolver decorator before event sourcing creates the event store.
     * </p>
     */
    public static final int WORKFLOW_DEFAULTS_ENHANCER_ORDER = EventSourcingConfigurationDefaults.ENHANCER_ORDER - 10;
    static final int DEFAULT_WORKFLOW_TIMER_THREAD_COUNT = 4;

    private static FutureResolver loadFutureResolver() {
        var contextClassLoader = Thread.currentThread().getContextClassLoader();
        var resolver = findFutureResolver(contextClassLoader);
        if (resolver == null && contextClassLoader != FutureResolver.class.getClassLoader()) {
            resolver = findFutureResolver(FutureResolver.class.getClassLoader());
        }
        return resolver != null ? resolver : new DefaultTimeoutFutureResolver();
    }

    @Nullable
    private static FutureResolver findFutureResolver(@Nullable ClassLoader classLoader) {
        if (classLoader == null) {
            return null;
        }
        Iterator<FutureResolver> resolvers = ServiceLoader.load(FutureResolver.class, classLoader).iterator();
        return resolvers.hasNext() ? resolvers.next() : null;
    }

    static ScheduledThreadPoolExecutor defaultWorkflowTimerExecutor() {
        return new ScheduledThreadPoolExecutor(
                DEFAULT_WORKFLOW_TIMER_THREAD_COUNT,
                runnable -> {
                    var thread = new Thread(runnable, "axon-workflow-timer");
                    thread.setDaemon(true);
                    return thread;
                }
        );
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        registerPayloadReducerRegistry(componentRegistry);
        registerFutureResolver(componentRegistry);
        registerEventNameCustomizer(componentRegistry);
        registerClock(componentRegistry);
        decorateTagResolver(componentRegistry);
        registerWorkflowStateModule(componentRegistry);
        registerExecuteStepActionResolver(componentRegistry);
        registerWorkflowTimeoutScheduler(componentRegistry);
        registerRunningWorkflowsModule(componentRegistry);
        registerWorkflowEngineExecutor(componentRegistry);
        registerWorkflowStore(componentRegistry);
        registerWorkflowIndex(componentRegistry);
    }

    private void registerPayloadReducerRegistry(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(PayloadReducerRegistry.class, cfg -> new PayloadReducerRegistry());
    }

    void registerFutureResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(FutureResolver.class, cfg -> loadFutureResolver());
    }

    void registerEventNameCustomizer(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                EventNameCustomizer.class,
                cfg -> DefaultEventNameCustomizer.Builder.defaults()
        );
    }

    void registerClock(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> ClockUtils.get());
    }

    void registerExecuteStepActionResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ExecuteStepActionResolver.class,
                cfg -> new DefaultExecuteStepActionResolver()
        );
    }

    void registerWorkflowTimeoutScheduler(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ComponentDefinition.ofType(WorkflowScheduler.class)
                                   .withBuilder(cfg -> new DefaultWorkflowScheduler(
                                           cfg.getComponent(Clock.class), defaultWorkflowTimerExecutor()
                                   ))
                                   .onShutdown(
                                           Phase.INBOUND_EVENT_CONNECTORS,
                                           scheduler -> ((DefaultWorkflowScheduler) scheduler).shutdown()
                                   )
        );
    }

    void decorateTagResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                DecoratorDefinition.forType(TagResolver.class)
                                   .with((cfg, name, delegate) -> new MultiTagResolver(
                                           delegate, new WorkflowEventTagResolver()
                                   ))
        );
    }

    void registerRunningWorkflowsModule(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(WorkflowEntityModule.runningWorkflows());
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> ClockUtils.get());
    }

    void registerWorkflowStateModule(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(WorkflowEntityModule.workflowState());
    }

    void registerWorkflowEngineExecutor(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ExecutorService.class,
                WORKFLOW_ENGINE_EXECUTOR,
                cfg -> Executors.newVirtualThreadPerTaskExecutor());
    }

    void registerWorkflowStore(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                WorkflowStore.class, cfg -> {
                    var repositories = cfg.getComponents(Repository.class).values();
                    @SuppressWarnings("unchecked")
                    var runningWorkflowsRepository = (Repository<String, EventSourcedRunningWorkflows>) repositories
                            .stream()
                            .filter(repository -> repository.entityType()
                                                            .equals(EventSourcedRunningWorkflows.class))
                            .findFirst()
                            .orElseThrow(() -> new NoSuchElementException(
                                    "No repository found for %s".formatted(EventSourcedRunningWorkflows.class.getName())));
                    @SuppressWarnings("unchecked")
                    var workflowStateRepository = (Repository<String, EventSourcedWorkflowState>) repositories
                            .stream()
                            .filter(repository -> repository.entityType()
                                                            .equals(EventSourcedWorkflowState.class))
                            .findFirst()
                            .orElseThrow(() -> new NoSuchElementException(
                                    "No repository found for %s".formatted(
                                            EventSourcedWorkflowState.class.getName()
                                    )));
                    return new EventSourcedWorkflowStore(runningWorkflowsRepository, workflowStateRepository);
                });
    }

    void registerWorkflowIndex(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(WorkflowIndex.class, cfg -> WorkflowIndex.none());
    }

    @Override
    public int order() {
        return WORKFLOW_DEFAULTS_ENHANCER_ORDER;
    }
}
