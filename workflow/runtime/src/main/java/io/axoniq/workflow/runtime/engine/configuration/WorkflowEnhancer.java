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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.engine.registry.SimpleWorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.api.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.repository.InMemoryWorkflowExecutionRepository;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.axoniq.workflow.runtime.engine.configuration.AllEventEventHandlingComponent.ANY_EVENT_IN_ONE_SEGMENT;

/**
 * Enhancer for registration of the workflow engine, the registry and sets up the eventing.
 */
public class WorkflowEnhancer implements ConfigurationEnhancer {

    public static final String WORKFLOW_ENGINE_EVENT_MODULE = "WorkflowEngine";
    public static final String WORKFLOW_ENGINE_EXECUTOR = "WorkflowEngine";

    @Override
    public void enhance(@NotNull ComponentRegistry componentRegistry) {

        componentRegistry
                .registerComponent(WorkflowDefinitionRegistry.class, cfg -> new SimpleWorkflowDefinitionRegistry());

        componentRegistry
                .registerComponent(EventNameCustomizer.class, cfg -> DefaultEventNameCustomizer.Builder.eventName());

        componentRegistry
                .registerComponent(Clock.class, cfg -> Clock.systemUTC());

        componentRegistry
                .registerComponent(Executor.class,
                                   WORKFLOW_ENGINE_EXECUTOR,
                                   cfg -> Executors.newVirtualThreadPerTaskExecutor());

        componentRegistry
                .registerComponent(WorkflowExecutionRepository.class, cfg -> new InMemoryWorkflowExecutionRepository());

        componentRegistry
                .registerComponent(WorkflowEngine.class, cfg ->
                        new WorkflowEngine(
                                cfg.getComponent(UnitOfWorkFactory.class),
                                cfg.getComponent(EventSink.class),
                                cfg.getComponent(WorkflowDefinitionRegistry.class),
                                cfg.getComponent(WorkflowExecutionRepository.class)
                        )
                );
        componentRegistry.registerModule(
                EventProcessorModule
                        .pooledStreaming(WORKFLOW_ENGINE_EVENT_MODULE)
                        .eventHandlingComponents(req -> req.declarative(cfg -> new AllEventEventHandlingComponent(
                                cfg.getComponent(WorkflowEngine.class)
                        )))
                        .customized(ANY_EVENT_IN_ONE_SEGMENT)
                        .build()
        );
    }
}
