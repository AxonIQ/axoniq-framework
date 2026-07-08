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
import io.axoniq.workflow.runtime.execution.InMemorySafePointStore;
import io.axoniq.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.RunningWorkflows;
import io.axoniq.workflow.runtime.execution.SafePointStore;
import io.axoniq.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.TokenStoreSafePointStore;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.WorkflowStateParameterResolverFactory;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import io.axoniq.workflow.runtime.util.WorkflowEventTagResolver;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.CriteriaResolver;
import org.axonframework.eventsourcing.EventSourcedEntityFactory;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.MultiTagResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.handler.EntityLifecycleHandler;
import org.axonframework.eventsourcing.handler.InitializingEntityEvolver;
import org.axonframework.eventsourcing.handler.SimpleEntityLifecycleHandler;
import org.axonframework.eventsourcing.handler.SnapshottingEntityLifecycleHandler;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.TokenSchema;
import org.axonframework.modelling.repository.Repository;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        registerTagResolver(componentRegistry);
        registerRunningWorkflows(componentRegistry);
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
        //  Issue AxonIQ/AxonFramework#3083 will introduce an ApplicationConfigurer wide Clock,
        //  which should replace the GenericEventMessage and subsequently this Clock.
        //noinspection deprecation
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> GenericEventMessage.clock);
    }

    void registerTagResolver(ComponentRegistry componentRegistry) {
        componentRegistry.registerDecorator(
                TagResolver.class,
                0,
                (cfg, name, delegate) -> new MultiTagResolver(delegate, new WorkflowEventTagResolver())
        );
    }

    void registerRunningWorkflows(ComponentRegistry componentRegistry) {
        var type = new TypeReference<Repository<String, RunningWorkflows>>() {
        };
        componentRegistry.registerIfNotPresent(
                ComponentDefinition.ofTypeAndName(type, RunningWorkflows.componentName())
                                   .withBuilder(c -> new EventSourcingRepository<>(
                                           String.class,
                                           RunningWorkflows.class,
                                           runningWorkflowsLifecycleHandler(c)
                                   ))
        );
    }

    private EntityLifecycleHandler<String, RunningWorkflows> runningWorkflowsLifecycleHandler(
            Configuration configuration
    ) {
        EventStore eventStore = configuration.getComponent(EventStore.class);
        CriteriaResolver<String> criteriaResolver = (identifier, context) ->
                RunningWorkflows.workflowLifecycleEvents(identifier);
        EventSourcedEntityFactory<String, RunningWorkflows> entityFactory =
                (identifier, firstEvent, context) -> new RunningWorkflows();
        var evolver = new InitializingEntityEvolver<>(
                entityFactory,
                (entity, eventMessage, context) -> entity.evolve(eventMessage.metadata())
        );

        // @formatter:off
        return configuration.getOptionalComponent(SnapshotPolicy.class, RunningWorkflows.componentName())
                  .<EntityLifecycleHandler<String, RunningWorkflows>>map(snapshotPolicy ->
                          new SnapshottingEntityLifecycleHandler<>(
                                  eventStore,
                                  criteriaResolver,
                                  evolver,
                                  snapshotPolicy,
                                  configuration
                                          .getOptionalComponent(MessageTypeResolver.class)
                                          .flatMap(resolver -> resolver.resolve(RunningWorkflows.class))
                                          .orElseThrow(() -> new IllegalStateException(
                                                "A MessageTypeResolver entry for RunningWorkflows is required to use snapshotting."
                                          )),
                                  configuration.getOptionalComponent(GeneralConverter.class)
                                     .orElseThrow(() -> new IllegalStateException(
                                             "A Converter must be configured to use snapshotting for RunningWorkflows."
                                     )),
                                  RunningWorkflows.class,
                                  configuration.getOptionalComponent(SnapshotStore.class)
                                     .orElseThrow(() -> new IllegalStateException(
                                             "A SnapshotStore must be configured to use snapshotting for RunningWorkflows."
                                     ))
                          ))
                  .orElseGet(() -> new SimpleEntityLifecycleHandler<>(
                          eventStore,
                          criteriaResolver,
                          evolver
                  ));
        // @formatter:on
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
                                cfg.getComponent(SafePointStore.class, COMPONENT_SAFE_POINT_STORE)
                        ))
                        .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, WorkflowEngine::shutdown)
        );
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
