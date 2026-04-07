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
package io.axoniq.workflow.runtime.test;

import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.execution.DSLAdoptingExecutionFactory;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.UnaryOperator;

/**
 * Abstract test base for workflow test, until we develop a test fixture.
 *
 * @param <T> type of the workflow context.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public abstract class AbstractDeclarativeTestBase<T extends WorkflowContext> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final Class<T> dslType;
    private final ComponentBuilder<WorkflowContextFactory<T>> builder;
    protected AxonConfiguration configuration;
    protected WorkflowEngine workflowEngine;
    protected DelayedPublisher delayedPublisher;
    protected WorkflowConfigurationRegistry<?> workflowRegistry;
    protected WorkflowHistoryRepository workflowHistoryRepository;

    public AbstractDeclarativeTestBase(@Nonnull Class<T> dslType,
                                       @Nonnull ComponentBuilder<WorkflowContextFactory<T>> contextFactoryBuilder) {
        this.dslType = dslType;
        this.builder = contextFactoryBuilder;
    }

    @BeforeEach
    void setUp() {

        var configurer = MessagingConfigurer.create();

        configurer.componentRegistry(r -> r.registerModule(
                                             WorkflowModule
                                                     .usingContext(dslType)
                                                     .workflowContextFactory(builder)
                                                     .workflowExecutionFactory(c -> new DSLAdoptingExecutionFactory<>(dslType))
                                                     .definitions(getDeclaredDefinitions())
                                     )
        );

        configuration = configurer.start();
        workflowEngine = configuration.getComponent(WorkflowEngine.class);
        workflowRegistry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        delayedPublisher = configuration.getComponent(DelayedPublisher.class);
        workflowHistoryRepository = configuration.getComponent(MutableWorkflowHistoryRepository.class);
    }

    protected abstract UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<T>> getDeclaredDefinitions();

    @AfterEach
    void shutdown() {
        var descriptor = new FilesystemStyleComponentDescriptor();
        configuration.getComponent(EventSink.class).describeTo(descriptor);
        workflowRegistry.describeTo(descriptor);
        logger.info(descriptor.describe());
        workflowEngine.shutdown();
        configuration.shutdown();
        ((MutableWorkflowHistoryRepository) workflowHistoryRepository).clear();
    }
}