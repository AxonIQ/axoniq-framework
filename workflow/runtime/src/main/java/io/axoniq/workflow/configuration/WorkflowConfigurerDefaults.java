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
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;

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
public class WorkflowConfigurerDefaults implements ConfigurationEnhancer {

    /**
     * Name of the event handling component used for workflow history projector.
     */
    public static final String COMPONENT_WORKFLOW_HISTORY_PROJECTOR = "WorkflowHistoryProjector";
    /**
     * Name of the event handling component used for workflow engine.
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
    public void enhance(@Nonnull ComponentRegistry componentRegistry) {

        registerEventNameCustomizer(componentRegistry);
        registerClock(componentRegistry);
        registerWorkflowEngineExecutor(componentRegistry);
        registerWorkflowExecutionRepository(componentRegistry);
        registerMutableWorkflowHistoryRepository(componentRegistry);
        registerWorkflowConfigurationRegistry(componentRegistry);
        registerWorkflowEngine(componentRegistry);
        registerWorkflowHistoryProjector(componentRegistry);
    }

    void registerEventNameCustomizer(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(EventNameCustomizer.class,
                                               cfg -> DefaultEventNameCustomizer.Builder.defaults());
    }

    void registerClock(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(Clock.class, cfg -> Clock.systemUTC());
    }

    void registerWorkflowEngineExecutor(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(ExecutorService.class,
                                               WORKFLOW_ENGINE_EXECUTOR,
                                               cfg -> Executors.newVirtualThreadPerTaskExecutor());
    }

    void registerWorkflowEngine(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(WorkflowEngine.class,
                                               cfg -> new WorkflowEngine(
                                                       cfg.getComponent(WorkflowConfigurationRegistry.class),
                                                       cfg.getComponent(WorkflowExecutionRepository.class)
                                               ));
    }

    void registerWorkflowHistoryProjector(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerIfNotPresent(WorkflowHistoryProjector.class,
                                               cfg -> new WorkflowHistoryProjector(
                                                       cfg.getComponent(MutableWorkflowHistoryRepository.class)
                                               ));
    }

    void registerWorkflowExecutionRepository(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry
                .registerComponent(WorkflowExecutionRepository.class,
                                   cfg -> new InMemoryWorkflowExecutionRepository());
    }

    void registerMutableWorkflowHistoryRepository(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry
                .registerIfNotPresent(MutableWorkflowHistoryRepository.class,
                                      cfg -> new InMemoryWorkflowHistoryRepository());
    }

    void registerWorkflowConfigurationRegistry(@Nonnull ComponentRegistry componentRegistry) {
        componentRegistry.registerComponent(WorkflowConfigurationRegistry.class,
                                            cfg -> new SimpleWorkflowConfigurationRegistry());
    }

    @Override
    public int order() {
        return WORKFLOW_DEFAULTS_ENHANCER_ORDER;
    }
}
