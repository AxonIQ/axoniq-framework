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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState;
import io.axoniq.workflow.runtime.execution.InMemorySafePointStore;
import io.axoniq.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.RepositoryBackedWorkflowStateRehydrationSupport;
import io.axoniq.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.workflow.runtime.execution.SafePointStore;
import io.axoniq.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.TokenStoreSafePointStore;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.WorkflowStateParameterResolverFactory;
import io.axoniq.workflow.runtime.execution.WorkflowStateRehydrationSupport;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.workflow.runtime.util.WorkflowEventTagResolver;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.eventsourcing.eventstore.MultiTagResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.TokenSchema;
import org.axonframework.modelling.repository.Repository;

import java.time.Clock;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.util.MetadataUtils.getWorkflowDefinitionId;

/**
 * Defaults for workflow configuration.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
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
     * Name of the component used for the workflow engine safe point tracking token store.
     */
    public static final String COMPONENT_SAFE_POINT_STORE = "WorkflowEngineSafePointStore";
    /**
     * Name of the component used for the workflow engine token store used for safe point persistence.
     */
    public static final String COMPONENT_SAFE_POINT_TOKEN_STORE = "WorkflowEngineSafePointTokenStore";

    /**
     * Token JDBC schema used for the {@link TokenStore}.
     */
    public static final TokenSchema SAFE_POINT_TOKEN_STORE_JDBC_SCHEMA = TokenSchema.builder()
                                                                                    .setTokenTable("WF_TOKEN_ENTRY")
                                                                                    .setProcessorNameColumn(
                                                                                            "PROCESSOR_NAME")
                                                                                    .setTokenTypeColumn("TOKEN_TYPE")
                                                                                    .setTokenColumn("TOKEN")
                                                                                    .setMaskColumn("MASK")
                                                                                    .setOwnerColumn("OWNER")
                                                                                    .setTimestampColumn("TIMESTAMP")
                                                                                    .setSegmentColumn("SEGMENT")
                                                                                    .build();
    /**
     * Name of the executor service component.
     */
    public static final String WORKFLOW_ENGINE_EXECUTOR = "WorkflowEngineExecutor";
    /**
     * Order for this enhancer.
     * <p>
     * Enhancer math: we have to run AFTER the event souring part is set up and let some space for others to register.
     * </p>
     */
    public static final int WORKFLOW_DEFAULTS_ENHANCER_ORDER = EventSourcingConfigurationDefaults.ENHANCER_ORDER + 50;

    /**
     * Registers default components.
     *
     * @param componentRegistry registry to use.
     */
    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        registerPayloadReducerRegistry(componentRegistry);
        registerEventNameCustomizer(componentRegistry);
        registerClock(componentRegistry);
        registerRunningWorkflows(componentRegistry);
        decorateTagResolver(componentRegistry);
        registerWorkflowStateRepository(componentRegistry);
        registerWorkflowEngineExecutor(componentRegistry);
        registerWorkflowExecutionRepository(componentRegistry);
        registerMutableWorkflowHistoryRepository(componentRegistry);
        registerWorkflowConfigurationRegistry(componentRegistry);
        registerSafePointStore(componentRegistry);
        registerWorkflowEngine(componentRegistry);
        registerWorkflowHistoryProjector(componentRegistry);
        registerWorkflowStateParameterResolverFactory(componentRegistry);
    }

    private void registerPayloadReducerRegistry(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(PayloadReducerRegistry.class,
                                               cfg -> new PayloadReducerRegistry());
    }

    void registerEventNameCustomizer(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(EventNameCustomizer.class,
                                               cfg -> DefaultEventNameCustomizer.Builder.defaults());
    }

    void registerClock(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> Clock.systemUTC());
    }

    void decorateTagResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                TagResolver.class,
                0,
                (cfg, name, delegate) -> new MultiTagResolver(delegate, new WorkflowEventTagResolver())
        );
    }

    void registerRunningWorkflows(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                EventSourcedEntityModule
                        .declarative(String.class, RunningWorkflows.class)
                        .messagingModel((c, model) -> model
                                .entityEvolver((entity, event, context) -> {
                                    entity.evolve(event.metadata());
                                    return entity;
                                })
                                .build())
                        .entityFactory(c -> (identifier, firstEvent, context) -> new RunningWorkflows())
                        .criteriaResolver(c -> (identifier, context) ->
                                RunningWorkflows.workflowLifecycleEvents())
                        // FIXME Register snapshot configuration eventually
                        .build()
        );
    }


    void registerWorkflowStateRepository(ComponentRegistry componentRegistry) {

        componentRegistry.registerModule(
                EventSourcedEntityModule
                        .declarative(String.class, EventSourcedWorkflowState.class)
                        .messagingModel((c, model) -> model
                                .entityEvolver((entity, event, context) -> (EventSourcedWorkflowState) entity.evolve(
                                        event,
                                        context))
                                .build())
                        .entityFactory(c -> (identifier, firstEvent, context) ->
                                new EventSourcedWorkflowState(
                                        identifier,
                                        getWorkflowDefinitionId(firstEvent.metadata()).orElseThrow(() -> new IllegalStateException(
                                                "Workflow state for '%s' cannot be created without workflowDefinitionId metadata."
                                                        .formatted(
                                                                identifier)
                                        ))
                                )
                        ).criteriaResolver(c -> (identifier, context) ->
                                EventSourcedWorkflowState.workflowEvents(identifier))
                        .build()

        );
    }

    void registerWorkflowEngineExecutor(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(ExecutorService.class,
                                               WORKFLOW_ENGINE_EXECUTOR,
                                               cfg -> Executors.newVirtualThreadPerTaskExecutor());
    }

    void registerWorkflowEngine(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(
                ComponentDefinition
                        .ofType(WorkflowEngine.class)
                        .withBuilder(cfg -> new WorkflowEngine(
                                cfg.getComponent(WorkflowConfigurationRegistry.class),
                                cfg.getComponent(WorkflowExecutionRepository.class),
                                cfg.getComponent(SafePointStore.class, COMPONENT_SAFE_POINT_STORE),
                                workflowStateRehydrationSupport(cfg)
                        ))
                        .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, WorkflowEngine::shutdown)
        );
    }

    private WorkflowStateRehydrationSupport workflowStateRehydrationSupport(Configuration configuration) {
        return new RepositoryBackedWorkflowStateRehydrationSupport(
                runningWorkflowsRepository(configuration),
                workflowStateRepository(configuration)
        );
    }

    @SuppressWarnings("unchecked")
    private Repository<String, RunningWorkflows> runningWorkflowsRepository(Configuration configuration) {
        return configuration.getComponents(Repository.class)
                            .values()
                            .stream()
                            .filter(repository -> repository.entityType().equals(RunningWorkflows.class))
                            .map(repository -> (Repository<String, RunningWorkflows>) repository)
                            .findFirst()
                            .orElseThrow(() -> new NoSuchElementException(
                                    "No repository found for %s".formatted(RunningWorkflows.class.getName())
                            ));
    }

    @SuppressWarnings("unchecked")
    private Repository<String, EventSourcedWorkflowState> workflowStateRepository(Configuration configuration) {
        return configuration.getComponents(Repository.class)
                            .values()
                            .stream()
                            .filter(repository -> repository.entityType().equals(EventSourcedWorkflowState.class))
                            .map(repository -> (Repository<String, EventSourcedWorkflowState>) repository)
                            .findFirst()
                            .orElseThrow(() -> new NoSuchElementException(
                                    "No repository found for %s".formatted(EventSourcedWorkflowState.class.getName())
                            ));
    }

    void registerSafePointStore(ComponentRegistry componentRegistry) {
        componentRegistry
                .registerIfNotPresent(
                        SafePointStore.class,
                        COMPONENT_SAFE_POINT_STORE,
                        cfg -> cfg.getOptionalComponent(
                                          TokenStore.class,
                                          COMPONENT_SAFE_POINT_TOKEN_STORE
                                  )
                                  .<SafePointStore>map(tokenStore ->
                                                               new TokenStoreSafePointStore(
                                                                       tokenStore,
                                                                       TokenStoreSafePointStore.tokenStoreIdentifier(
                                                                               WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME
                                                                       )
                                                               ))
                                  .orElseGet(InMemorySafePointStore::new)
                );
    }

    void registerWorkflowHistoryProjector(ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(WorkflowHistoryProjector.class,
                                               cfg -> new WorkflowHistoryProjector(
                                                       cfg.getComponent(MutableWorkflowHistoryRepository.class)
                                               ));
    }

    void registerWorkflowExecutionRepository(ComponentRegistry componentRegistry) {
        componentRegistry
                .registerComponent(WorkflowExecutionRepository.class,
                                   cfg -> new InMemoryWorkflowExecutionRepository());
    }

    void registerMutableWorkflowHistoryRepository(ComponentRegistry componentRegistry) {
        componentRegistry
                .registerIfNotPresent(MutableWorkflowHistoryRepository.class,
                                      cfg -> new InMemoryWorkflowHistoryRepository());
    }

    void registerWorkflowConfigurationRegistry(ComponentRegistry componentRegistry) {
        componentRegistry.registerComponent(WorkflowConfigurationRegistry.class,
                                            cfg -> new SimpleWorkflowConfigurationRegistry());
    }

    void registerWorkflowStateParameterResolverFactory(ComponentRegistry componentRegistry) {
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                WorkflowStateParameterResolverFactory::new
        );
    }

    @Override
    public int order() {
        return WORKFLOW_DEFAULTS_ENHANCER_ORDER;
    }
}
