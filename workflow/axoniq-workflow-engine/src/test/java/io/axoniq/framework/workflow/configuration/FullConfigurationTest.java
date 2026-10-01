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

package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for full configuration of workflow modules.
 *
 * @author Simon Zambrovski
 */
class FullConfigurationTest {

    @Test
    void configurationWithDefaults() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        var module = WorkflowModule.defaults("defaults-module", TestContext.class)
                                   .contextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> (ctx) -> {
                                           })
                                           .workflowName("wf-defaults")
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   );

        configurer.componentRegistry(cr -> cr.registerModule(module));
        AxonConfiguration configuration = configurer.build();

        // The module registers its own, default-implementation components, named after itself.
        assertThat(configuration.getComponents(WorkflowConfigurationRegistry.class)
                                .get("WorkflowConfigurationRegistry[defaults-module]"))
                .isInstanceOf(SimpleWorkflowConfigurationRegistry.class);
        assertThat(configuration.getComponents(WorkflowExecutionRepository.class)
                                .get("WorkflowExecutionRepository[defaults-module]"))
                .isInstanceOf(InMemoryWorkflowExecutionRepository.class);
    }

    @Test
    void testConfigurationWithCustomComponents() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        WorkflowConfigurationRegistry<?> customRegistry = new SimpleWorkflowConfigurationRegistry();
        WorkflowExecutionRepository customRepository = new InMemoryWorkflowExecutionRepository();

        var module = WorkflowModule.configure("custom-module", TestContext.class)
                                   .withoutHistory()
                                   .contextFactory(c -> TestContext::new)
                                   .definition(d -> d
                                           .declarative(c -> (ctx) -> {
                                           })
                                           .workflowName("wf-custom")
                                           .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                           .notCustomized()
                                   )
                                   .configurationRegistry(cfg -> customRegistry)
                                   .executionRepository(cfg -> customRepository);

        configurer.componentRegistry(cr -> cr.registerModule(module));
        AxonConfiguration configuration = configurer.build();

        // The module registers the exact custom instances it was given, named after itself.
        assertThat(configuration.getComponents(WorkflowConfigurationRegistry.class)
                                .get("WorkflowConfigurationRegistry[custom-module]"))
                .isSameAs(customRegistry);
        assertThat(configuration.getComponents(WorkflowExecutionRepository.class)
                                .get("WorkflowExecutionRepository[custom-module]"))
                .isSameAs(customRepository);
    }

    static class TestContext extends AbstractWorkflowContext {

        public TestContext(Map<String, @Nullable Object> payload, String workflowId,
                           ProcessingContext processingContext,
                           WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
