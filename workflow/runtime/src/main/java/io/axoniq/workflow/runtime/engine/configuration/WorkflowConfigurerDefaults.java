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
package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.engine.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.history.InMemoryWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.engine.history.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.engine.history.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.engine.registry.SimpleWorkflowConfigurationRegistry;
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
// @RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public class WorkflowConfigurerDefaults implements ConfigurationEnhancer {

    /**
     * Name of the executor service component.
     */
    public static final String WORKFLOW_ENGINE_EXECUTOR = "WorkflowEngine";
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

        componentRegistry
                .registerIfNotPresent(WorkflowConfigurationRegistry.class,
                                      cfg -> new SimpleWorkflowConfigurationRegistry());

        componentRegistry
                .registerIfNotPresent(EventNameCustomizer.class, cfg -> DefaultEventNameCustomizer.Builder.defaults());

        componentRegistry
                .registerIfNotPresent(Clock.class, cfg -> Clock.systemUTC());

        componentRegistry
                .registerIfNotPresent(ExecutorService.class,
                                      WORKFLOW_ENGINE_EXECUTOR,
                                      cfg -> Executors.newVirtualThreadPerTaskExecutor());

        componentRegistry
                .registerIfNotPresent(WorkflowExecutionRepository.class,
                                      cfg -> new InMemoryWorkflowExecutionRepository());

        componentRegistry
                .registerIfNotPresent(WorkflowEngine.class, cfg ->
                        new WorkflowEngine(
                                cfg.getComponent(WorkflowConfigurationRegistry.class),
                                cfg.getComponent(WorkflowExecutionRepository.class)
                        )
                );
        componentRegistry
                .registerIfNotPresent(MutableWorkflowHistoryRepository.class,
                                      cfg -> new InMemoryWorkflowHistoryRepository());

        componentRegistry
                .registerIfNotPresent(WorkflowHistoryProjector.class, cfg -> new WorkflowHistoryProjector(
                        cfg.getComponent(MutableWorkflowHistoryRepository.class)
                ));
    }

    @Override
    public int order() {
        return WORKFLOW_DEFAULTS_ENHANCER_ORDER;
    }
}
