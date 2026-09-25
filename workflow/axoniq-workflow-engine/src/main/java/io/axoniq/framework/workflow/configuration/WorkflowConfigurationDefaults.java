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
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.DefaultExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.DefaultWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowStore;
import io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineCheckpointingSupport;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStateParameterResolverFactory;
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
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;

import java.time.Clock;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowDefinitionId;
import static org.axonframework.eventsourcing.configuration.EventSourcedEntityModule.declarative;

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
     * Name of the event handling component used for workflow history projector.
     */
    public static final String COMPONENT_WORKFLOW_HISTORY_PROJECTOR = "WorkflowHistoryProjector";
    /**
     * Name of the event handling component used for the workflow engine.
     */
    public static final String COMPONENT_WORKFLOW_ENGINE = "WorkflowEngine";
    /**
     * Name of the dedicated executor service component used for workflow-body work and workflow-event publication.
     * <p>
     * The default uses a virtual thread per task. A bounded future-resolution wait therefore blocks only its workflow
     * task, not an event processor or another shared executor. Applications that replace this component should retain
     * that isolation or size their executor for the configured resolver timeout.
     */
    public static final String WORKFLOW_ENGINE_EXECUTOR = "WorkflowEngineExecutor";
    /**
     * Order for this enhancer.
     * <p>
     * Enhancer math: register the tag-resolver decorator before event sourcing creates the event store.
     * </p>
     */
    public static final int WORKFLOW_DEFAULTS_ENHANCER_ORDER = EventSourcingConfigurationDefaults.ENHANCER_ORDER - 10;
    static final int DEFAULT_WORKFLOW_TIMER_THREAD_COUNT = 4;
    /**
     * Phase in which the engine's executions are dropped on shutdown: a workflow is a message handler, extensively
     * wrapped, so it is dropped where message handlers are.
     * <p>
     * Shutdown handlers run from the highest phase down, so this runs after the event processor has stopped at
     * {@link Phase#INBOUND_EVENT_CONNECTORS} and its drain has stored the token. Sharing the processor's own phase
     * would make the two race: clearing the repository first leaves the drain with nothing to hold the token back, and
     * it stores a position whose wakes were never applied.
     */
    private static final int POST_PROCESSOR_SHUTDOWN_PHASE = Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS;

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

    /**
     * Registers default components.
     *
     * @param componentRegistry registry to use.
     */
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
        registerWorkflowExecutionRepository(componentRegistry);
        registerWorkflowCancellationService(componentRegistry);
        registerMutableWorkflowHistoryRepository(componentRegistry);
        registerWorkflowManager(componentRegistry);
        registerWorkflowConfigurationRegistry(componentRegistry);
        registerWorkflowStore(componentRegistry);
        registerWorkflowEngine(componentRegistry);
        registerWorkflowHistoryProjector(componentRegistry);
        registerWorkflowStateParameterResolverFactory(componentRegistry);
        registerCheckpointingSupport(componentRegistry);
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
                cfg -> DefaultEventNameCustomizer.Builder.defaults());
    }

    void registerClock(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> ClockUtils.get());
    }

    void registerExecuteStepActionResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ExecuteStepActionResolver.class,
                cfg -> new DefaultExecuteStepActionResolver());
    }

    void registerWorkflowTimeoutScheduler(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ComponentDefinition.ofType(WorkflowScheduler.class)
                                   .withBuilder(cfg -> new DefaultWorkflowScheduler(
                                           cfg.getComponent(Clock.class), defaultWorkflowTimerExecutor()
                                   ))
                                   .onShutdown(Phase.INBOUND_EVENT_CONNECTORS,
                                               scheduler -> ((DefaultWorkflowScheduler) scheduler).shutdown()));
    }

    void decorateTagResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                DecoratorDefinition
                        .forType(TagResolver.class)
                        .with((cfg, name, delegate) -> new MultiTagResolver(delegate, new WorkflowEventTagResolver()))
        );
    }

    void registerRunningWorkflowsModule(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                declarative(String.class, EventSourcedRunningWorkflows.class)
                        .messagingModel((c, model) -> model.entityEvolver((entity, event, context) -> {
                            entity.evolve(event.metadata());
                            return entity;
                        }).build())
                        .entityFactory(c -> (identifier, firstEvent, context) -> new EventSourcedRunningWorkflows())
                        .criteriaResolver(c -> (identifier, context) -> EventSourcedRunningWorkflows.criteriaBuilder())
                        // FIXME Register snapshot configuration eventually, see #245
                        .build());
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> ClockUtils.get());
    }

    void registerWorkflowStateModule(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                declarative(String.class, EventSourcedWorkflowState.class)
                        .messagingModel((c, model) -> model.entityEvolver((entity, event, context) ->
                                                                                  EventSourcedWorkflowState.requireEventSourcedState(
                                                                                          entity.evolve(event, context))
                        ).build())
                        .entityFactory(c -> (identifier, firstEvent, context) -> new EventSourcedWorkflowState(
                                identifier,
                                getWorkflowDefinitionId(firstEvent.metadata()).orElseThrow(
                                        () -> new IllegalStateException(
                                                "Workflow state for '%s' cannot be created without workflowDefinitionId metadata.".formatted(
                                                        identifier)))))
                        .criteriaResolver(c -> (identifier, context) -> EventSourcedWorkflowState.criteriaBuilder(
                                identifier))
                        // FIXME Register snapshot configuration eventually, see #245
                        .build());
    }

    void registerWorkflowEngineExecutor(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ExecutorService.class,
                WORKFLOW_ENGINE_EXECUTOR,
                cfg -> Executors.newVirtualThreadPerTaskExecutor());
    }

    void registerWorkflowEngine(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ComponentDefinition.ofType(WorkflowEngine.class)
                                   .withBuilder(cfg -> new WorkflowEngine(
                                           cfg.getComponent(WorkflowConfigurationRegistry.class),
                                           cfg.getComponent(WorkflowExecutionRepository.class),
                                           cfg.getComponent(WorkflowCancellationService.class),
                                           cfg.getComponent(WorkflowStore.class),
                                           cfg.getComponent(UnitOfWorkFactory.class)
                                   ))
                                   .onStart(Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS, (config, engine) -> {
                                       engine.setCheckpointingSupport(
                                               config.getComponent(WorkflowEngineCheckpointingSupport.class));
                                   })
                                   .onShutdown(POST_PROCESSOR_SHUTDOWN_PHASE,
                                               WorkflowEngine::shutdown));
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

    void registerWorkflowHistoryProjector(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                WorkflowHistoryProjector.class,
                cfg -> new WorkflowHistoryProjector(cfg.getComponent(
                        MutableWorkflowHistoryRepository.class)));
    }

    void registerWorkflowExecutionRepository(ComponentRegistry componentRegistry) {
        componentRegistry.registerComponent(
                WorkflowExecutionRepository.class,
                cfg -> new InMemoryWorkflowExecutionRepository());
    }

    void registerWorkflowCancellationService(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(WorkflowCancellationService.class,
                                               cfg -> new WorkflowCancellationService());
    }

    void registerWorkflowManager(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                WorkflowManager.class,
                cfg -> new SimpleWorkflowManager(
                        cfg.getComponent(WorkflowHistoryRepository.class),
                        cfg.getComponent(WorkflowExecutionRepository.class),
                        cfg.getComponent(WorkflowCancellationService.class),
                        cfg.getComponent(WorkflowStore.class),
                        cfg.getComponent(UnitOfWorkFactory.class),
                        cfg.getComponent(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR)
                )
        );
    }

    void registerMutableWorkflowHistoryRepository(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                MutableWorkflowHistoryRepository.class,
                cfg -> new InMemoryWorkflowHistoryRepository());
    }

    void registerWorkflowConfigurationRegistry(ComponentRegistry componentRegistry) {
        componentRegistry.registerComponent(
                WorkflowConfigurationRegistry.class,
                cfg -> new SimpleWorkflowConfigurationRegistry());
    }

    void registerWorkflowStateParameterResolverFactory(ComponentRegistry componentRegistry) {
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                WorkflowStateParameterResolverFactory::new);
    }

    void registerCheckpointingSupport(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(WorkflowEngineCheckpointingSupport.class,
                                               cfg -> new WorkflowEngineCheckpointingSupport(cfg.getComponent(
                                                       WorkflowEngine.class)
                                               ));
    }

    @Override
    public int order() {
        return WORKFLOW_DEFAULTS_ENHANCER_ORDER;
    }
}
