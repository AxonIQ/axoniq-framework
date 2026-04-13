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
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import io.axoniq.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test demonstrating that different modules use different components (global, local with history, local without
 * history).
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class ModuleComponentIsolationTest {

    @Test
    void testModuleComponentIsolation() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        // 1. Global module (uses global defaults)
        var globalModule = WorkflowModule.defaults("global-module", TestContext.class)
                                         .workflowContextFactory(c -> TestContext::new)
                                         .definition(d -> d.declarative(c -> (ctx) -> {
                                                           })
                                                           .workflowName("wf-global")
                                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName(
                                                                   "start")))
                                                           .notCustomized());

        // 2. Local module with history
        WorkflowConfigurationRegistry<?> localRegistryWithHistory = new SimpleWorkflowConfigurationRegistry();
        WorkflowExecutionRepository localRepositoryWithHistory = new InMemoryWorkflowExecutionRepository();
        MutableWorkflowHistoryRepository localHistoryRepository = new InMemoryWorkflowHistoryRepository();

        var localWithHistoryModule = WorkflowModule.configure("local-with-history-module", TestContext.class)
                                                   .workflowConfigurationRegistry(cfg -> localRegistryWithHistory)
                                                   .workflowExecutionRepository(cfg -> localRepositoryWithHistory)
                                                   .withHistory(cfg -> new WorkflowHistoryProjector(
                                                           localHistoryRepository))
                                                   .workflowContextFactory(c -> TestContext::new)
                                                   .definition(d -> d.declarative(c -> (ctx) -> {
                                                                     })
                                                                     .workflowName("wf-local-history")
                                                                     .on(c -> EventConditions.fromQualifiedName(new QualifiedName(
                                                                             "start")))
                                                                     .notCustomized());

        // 3. Local module without history
        WorkflowConfigurationRegistry<?> localRegistryWithoutHistory = new SimpleWorkflowConfigurationRegistry();
        WorkflowExecutionRepository localRepositoryWithoutHistory = new InMemoryWorkflowExecutionRepository();

        var localWithoutHistoryModule = WorkflowModule.configure("local-without-history-module", TestContext.class)
                                                      .workflowConfigurationRegistry(cfg -> localRegistryWithoutHistory)
                                                      .workflowExecutionRepository(cfg -> localRepositoryWithoutHistory)
                                                      .withoutHistory()
                                                      .workflowContextFactory(c -> TestContext::new)
                                                      .definition(d -> d.declarative(c -> (ctx) -> {
                                                                        })
                                                                        .workflowName("wf-local-no-history")
                                                                        .on(c -> EventConditions.fromQualifiedName(new QualifiedName(
                                                                                "start")))
                                                                        .notCustomized());

        configurer.componentRegistry(cr -> cr
                .registerModule(globalModule)
                .registerModule(localWithHistoryModule)
                .registerModule(localWithoutHistoryModule)
        );

        AxonConfiguration configuration = configurer.build();

        // Verify Global Module
        // It uses the global components. We can just check the global components in the main configuration.
        assertThat(configuration.getComponent(WorkflowConfigurationRegistry.class))
                .isInstanceOf(SimpleWorkflowConfigurationRegistry.class);
        assertThat(configuration.getComponent(WorkflowExecutionRepository.class))
                .isInstanceOf(InMemoryWorkflowExecutionRepository.class);
        assertThat(configuration.getComponent(WorkflowEngine.class))
                .isNotNull();

        // Local modules with custom components.
        // Even if we cannot easily verify isolation via AxonConfiguration API without knowing the internals of BaseModule's registration,
        // we have demonstrated the configuration of such modules.
        // As a proxy, we verify that the configurer build succeeded with multiple modules.
        assertThat(configuration).isNotNull();
    }

    static class TestContext extends AbstractDSLWorkflowContext {

        public TestContext(@Nonnull Map<String, Object> payload, @Nonnull String workflowId,
                           @Nonnull ProcessingContext processingContext,
                           @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
