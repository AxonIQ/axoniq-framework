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

import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import io.axoniq.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for full configuration of workflow modules.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class FullConfigurationTest {

    @Test
    void testConfigurationWithDefaults() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        var module = WorkflowModule.defaults("defaults-module", TestContext.class)
                                   .workflowContextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> (ctx) -> {
                                           })
                                           .workflowName("wf-defaults")
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );

        configurer.componentRegistry(cr -> cr.registerModule(module));
        AxonConfiguration configuration = configurer.build();

        // Should use global components
        assertThat(configuration.getComponent(WorkflowConfigurationRegistry.class)).isInstanceOf(
                SimpleWorkflowConfigurationRegistry.class);
        assertThat(configuration.getComponent(WorkflowExecutionRepository.class)).isInstanceOf(
                InMemoryWorkflowExecutionRepository.class);
    }

    @Test
    void testConfigurationWithCustomComponents() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        WorkflowConfigurationRegistry<?> customRegistry = new SimpleWorkflowConfigurationRegistry();
        WorkflowExecutionRepository customRepository = new InMemoryWorkflowExecutionRepository();

        var module = WorkflowModule.configure("custom-module", TestContext.class)
                                   .workflowConfigurationRegistry(cfg -> customRegistry)
                                   .workflowExecutionRepository(cfg -> customRepository)
                                   .withoutHistory()
                                   .workflowContextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> (ctx) -> {
                                           })
                                           .workflowName("wf-custom")
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );

        configurer.componentRegistry(cr -> cr.registerModule(module));
        AxonConfiguration configuration = configurer.build();

        // The module-local configuration should have these components.
        // We can verify that they are different from the global ones
        assertThat(configuration.getComponent(WorkflowConfigurationRegistry.class)).isNotSameAs(customRegistry);
        assertThat(configuration.getComponent(WorkflowExecutionRepository.class)).isNotSameAs(customRepository);

        // To verify module-local ones, we would need to access the module's component registry.
        // But we can check if they are registered in the global configuration under the module name if they were exported.
        // SimpleWorkflowModule doesn't export them with a name, but it registers them in the local registry.
    }

    static class TestContext extends AbstractDSLWorkflowContext {

        public TestContext(@Nonnull Map<String, Object> payload, @Nonnull String workflowId,
                           @Nonnull ProcessingContext processingContext,
                           @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
