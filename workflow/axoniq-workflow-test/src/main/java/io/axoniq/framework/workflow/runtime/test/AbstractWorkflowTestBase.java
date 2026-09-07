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
package io.axoniq.framework.workflow.runtime.test;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.test.fixture.WorkflowTestDriver;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
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
 * @since 5.4.0
 */
public abstract class AbstractWorkflowTestBase<T extends WorkflowContext> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final WorkflowTestDriver testDriver;
    protected AxonConfiguration configuration;
    protected WorkflowEngine workflowEngine;
    protected WorkflowCancellationService workflowCancellationService;
    protected DelayedPublisher delayedPublisher;
    protected WorkflowConfigurationRegistry<?> workflowRegistry;
    protected WorkflowHistoryRepository workflowHistoryRepository;

    /**
     * Creates a new test base.
     *
     * @param dslType dsl type to use
     * @param contextFactoryBuilder context factory builder
     */
    public AbstractWorkflowTestBase(Class<T> dslType,
                                    ComponentBuilder<WorkflowContextFactory<T>> contextFactoryBuilder) {
        var module = WorkflowModule.defaults(getClass().getSimpleName(), dslType)
                                   .workflowContextFactory(contextFactoryBuilder)
                                   .definition(getDeclaredDefinition());
        for (var extra : getAdditionalDefinitions()) {
            module = module.definition(extra);
        }
        this.testDriver = WorkflowTestDriver.live(module, configure());
    }

    @BeforeEach
    void setUp() {
        this.configuration = testDriver.workflowTestServices().configuration();
        this.workflowEngine = testDriver.workflowTestServices().workflowEngine();
        this.workflowCancellationService = configuration.getComponent(WorkflowCancellationService.class);
        this.delayedPublisher = testDriver.workflowTestServices().delayedPublisher();
        this.workflowRegistry = testDriver.workflowTestServices().workflowRegistry();
        this.workflowHistoryRepository = testDriver.workflowTestServices().workflowHistoryRepository();
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
     * Override to register additional workflow definitions alongside {@link #getDeclaredDefinition()} into the same
     * module. Use for multi-version test scenarios where two or more versions of a workflow need to share one engine
     * and registry (the production wiring for Spring Boot autoconfig).
     *
     * @return additional definitions, defaults to empty list.
     */
    protected List<Function<DetectionPhase<T>, FinalizedPhase<T>>> getAdditionalDefinitions() {
        return List.of();
    }

    @AfterEach
    void shutdown() {
        var descriptor = new FilesystemStyleComponentDescriptor();
        configuration.getComponent(EventSink.class).describeTo(descriptor);
        workflowRegistry.describeTo(descriptor);
        logger.info(descriptor.describe());

        testDriver.shutdown();
    }
}
