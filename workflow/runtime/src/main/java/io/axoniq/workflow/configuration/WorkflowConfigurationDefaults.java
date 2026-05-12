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
import io.axoniq.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.WorkflowStateParameterResolverFactory;
import io.axoniq.workflow.runtime.execution.payload.PayloadReducerRegistry;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

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
        registerWorkflowEngineExecutor(componentRegistry);
        registerWorkflowExecutionRepository(componentRegistry);
        registerMutableWorkflowHistoryRepository(componentRegistry);
        registerWorkflowConfigurationRegistry(componentRegistry);
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
                                cfg.getComponent(WorkflowExecutionRepository.class)
                        ))
                        .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, WorkflowEngine::shutdown)
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
