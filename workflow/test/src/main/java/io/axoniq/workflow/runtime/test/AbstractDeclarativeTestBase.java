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
package io.axoniq.workflow.runtime.test;

import io.axoniq.workflow.configuration.WorkflowConfigurer;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;
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

        var configurer = WorkflowConfigurer.create();

        configurer
                .componentRegistry(r -> r.registerModule(buildModule()));

        configuration = configure().apply(configurer).start();
        workflowEngine = configuration.getComponent(WorkflowEngine.class);
        workflowRegistry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        delayedPublisher = configuration.getComponent(DelayedPublisher.class);
        workflowHistoryRepository = configuration.getComponent(MutableWorkflowHistoryRepository.class);
    }

    /**
     * Allows further customization before the start of the configurer.
     *
     * @return modified configurer
     */
    protected UnaryOperator<WorkflowConfigurer> configure() {
        return UnaryOperator.identity();
    }

    protected abstract Function<DetectionPhase<T>, FinalizedPhase<T>> getDeclaredDefinition();

    /**
     * Override to register additional workflow definitions alongside {@link #getDeclaredDefinition()} into
     * the same module. Use for multi-version test scenarios where two or more versions of a workflow
     * need to share one engine + registry (the production wiring for Spring Boot autoconfig).
     *
     * @return additional definitions, defaults to empty list.
     */
    protected List<Function<DetectionPhase<T>, FinalizedPhase<T>>> getAdditionalDefinitions() {
        return List.of();
    }

    private WorkflowModule<T> buildModule() {
        WorkflowModule<T> module = WorkflowModule
                .defaults(getClass().getSimpleName(), dslType)
                .workflowContextFactory(builder)
                .definition(getDeclaredDefinition());
        for (var extra : getAdditionalDefinitions()) {
            module = module.definition(extra);
        }
        return module;
    }

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
