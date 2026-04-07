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

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Enhancer for registration of the workflow component.
 * For eventing see {@link WorkflowEventProcessingRegistrationEnhancer}.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
@Internal
public class WorkflowEnhancer implements ConfigurationEnhancer {

    public static final String WORKFLOW_ENGINE_EXECUTOR = "WorkflowEngine";

    /**
     * {@inheritDoc}
     * <p>
     * Registers the workflow engine, definition registry, execution repository, and event processing module into the
     * given {@link ComponentRegistry}.
     */
    @Override
    public void enhance(@Nonnull ComponentRegistry componentRegistry) {

        componentRegistry
                .registerComponent(WorkflowConfigurationRegistry.class,
                                   cfg -> new SimpleWorkflowConfigurationRegistry());

        componentRegistry
                .registerComponent(EventNameCustomizer.class, cfg -> DefaultEventNameCustomizer.Builder.defaults());

        componentRegistry
                .registerComponent(Clock.class, cfg -> Clock.systemUTC());

        componentRegistry
                .registerComponent(ExecutorService.class,
                                   WORKFLOW_ENGINE_EXECUTOR,
                                   cfg -> Executors.newVirtualThreadPerTaskExecutor());

        componentRegistry
                .registerComponent(WorkflowExecutionRepository.class, cfg -> new InMemoryWorkflowExecutionRepository());

        componentRegistry
                .registerComponent(MutableWorkflowHistoryRepository.class,
                                   cfg -> new InMemoryWorkflowHistoryRepository());

        componentRegistry
                .registerComponent(WorkflowEngine.class, cfg ->
                        new WorkflowEngine(
                                cfg.getComponent(WorkflowConfigurationRegistry.class),
                                cfg.getComponent(WorkflowExecutionRepository.class)
                        )
                );
        componentRegistry
                .registerComponent(WorkflowHistoryProjector.class, cfg -> new WorkflowHistoryProjector(
                        cfg.getComponent(MutableWorkflowHistoryRepository.class)
                ));
    }
}
