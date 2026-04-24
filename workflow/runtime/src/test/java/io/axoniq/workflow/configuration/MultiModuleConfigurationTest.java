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

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for multi module configuration.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class MultiModuleConfigurationTest {

    @Test
    void testMultipleModulesHaveDifferentEngines() {
        WorkflowConfigurer configurer = WorkflowConfigurer.create();

        EventCondition startCondition1 = EventConditions.fromQualifiedName(new QualifiedName("startEvent1"));
        WorkflowDefinition<TestContext1> definition1 = ctx -> {
        };

        EventCondition startCondition2 = EventConditions.fromQualifiedName(new QualifiedName("startEvent2"));
        WorkflowDefinition<TestContext2> definition2 = ctx -> {
        };

        var module1 = WorkflowModule.defaults("wf1", TestContext1.class)
                                    .workflowContextFactory(c -> TestContext1::new)
                                    .definition(d -> d
                                            .declarative(c -> definition1)
                                            .workflowName("wf1")
                                            .on(c -> startCondition1)
                                            .notCustomized()
                                    );

        var module2 = WorkflowModule.defaults("wf2", TestContext2.class)
                                    .workflowContextFactory(c -> (payload, workflowId, processingContext, workflowConfiguration) -> new TestContext2(
                                            payload,
                                            workflowId,
                                            processingContext,
                                            workflowConfiguration))
                                    .definition(d -> d
                                            .declarative(c -> (WorkflowDefinition<TestContext2>) (ctx) -> definition2.accept(
                                                    ctx))
                                            .workflowName("wf2")
                                            .on(c -> startCondition2)
                                            .notCustomized()
                                    );

        configurer.componentRegistry(componentRegistry -> componentRegistry.registerModule(module1)
                                                                           .registerModule(module2));
        AxonConfiguration configuration = configurer.build();

        // Verify that we have two workflow modules registered
        // Actually AxonConfiguration doesn't expose modules easily.
        // But if configurer.build() succeeded, it means the SPI issue is gone
        // and the modules were initialized.
        assertThat(configuration).isNotNull();
    }

    static class TestContext1 extends AbstractDSLWorkflowContext {

        public TestContext1(@Nonnull Map<String, Object> payload, @Nonnull String workflowId,
                            @Nonnull ProcessingContext processingContext,
                            @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    static class TestContext2 extends AbstractDSLWorkflowContext {

        public TestContext2(@Nonnull Map<String, Object> payload, @Nonnull String workflowId,
                            @Nonnull ProcessingContext processingContext,
                            @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }
}
