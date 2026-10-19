/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.execution.AbstractWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a {@link RecoverableWorkflowExceptionPolicy} reaches the workflow configuration through each configuration
 * path: the default, a registered component, and a per-workflow customization.
 *
 * @author Stefan Dragisic
 */
class WorkflowConfigurerRecoverableExceptionPolicyTest {

    private static final RecoverableWorkflowExceptionPolicy COMPONENT_POLICY = e -> e instanceof IllegalStateException;
    private static final RecoverableWorkflowExceptionPolicy WORKFLOW_POLICY = e -> e instanceof ArithmeticException;

    private static RecoverableWorkflowExceptionPolicy policyOf(WorkflowConfigurer configurer,
                                                               String moduleName,
                                                               String workflowName) {
        WorkflowConfigurationRegistry<?> registry =
                configurer.build()
                          .getComponents(WorkflowConfigurationRegistry.class)
                          .get("WorkflowConfigurationRegistry[" + moduleName + "]");
        return registry.findByWorkflowNameAndVersion(workflowName, "0.0.1")
                       .orElseThrow()
                       .recoverableExceptionPolicy();
    }

    private static WorkflowModule declarativeModule(boolean customizePolicy) {
        return WorkflowModule.defaults("declarative-module", TestContext.class)
                             .definition(d -> d
                                     .declarative(c -> ctx -> {
                                     })
                                     .workflowName("wf-declarative")
                                     .on(c -> EventConditions.fromQualifiedName(new QualifiedName("start")))
                                     .customized((c, w) -> customizePolicy
                                             ? w.recoverableExceptionPolicy(WORKFLOW_POLICY)
                                             : w)
                             )
                             .contextFactory(c -> TestContext::new);
    }

    private static WorkflowModule annotatedModule() {
        return WorkflowModule.defaults("annotated-module", TestContext.class)
                             .definition(d -> d.autodetected(c -> new AnnotatedTestWorkflow()))
                             .contextFactory(c -> TestContext::new);
    }

    public static class AnnotatedTestWorkflow {

        @Workflow(workflowName = "wf-annotated", startOnEventName = "java.lang.String", idProperty = "id")
        void run(TestContext context) {
        }
    }

    static class TestContext extends AbstractWorkflowContext {

        public TestContext(Map<String, @Nullable Object> payload, String workflowId,
                           ProcessingContext processingContext,
                           WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    @Nested
    class DeclarativeWorkflow {

        @Test
        void usesDefaultPolicyWithoutComponent() {
            // given
            var configurer = WorkflowConfigurer.create();
            configurer.componentRegistry(cr -> cr.registerModule(declarativeModule(false)));

            // when
            var policy = policyOf(configurer, "declarative-module", "wf-declarative");

            // then
            assertThat(policy).isSameAs(RecoverableWorkflowExceptionPolicy.DEFAULT);
        }

        @Test
        void usesRegisteredComponentPolicy() {
            // given
            var configurer = WorkflowConfigurer.create();
            configurer.componentRegistry(cr -> cr.registerComponent(RecoverableWorkflowExceptionPolicy.class,
                                                                    c -> COMPONENT_POLICY)
                                                 .registerModule(declarativeModule(false)));

            // when
            var policy = policyOf(configurer, "declarative-module", "wf-declarative");

            // then
            assertThat(policy).isSameAs(COMPONENT_POLICY);
        }

        @Test
        void perWorkflowPolicyOverridesComponentPolicy() {
            // given
            var configurer = WorkflowConfigurer.create();
            configurer.componentRegistry(cr -> cr.registerComponent(RecoverableWorkflowExceptionPolicy.class,
                                                                    c -> COMPONENT_POLICY)
                                                 .registerModule(declarativeModule(true)));

            // when
            var policy = policyOf(configurer, "declarative-module", "wf-declarative");

            // then
            assertThat(policy).isSameAs(WORKFLOW_POLICY);
        }
    }

    @Nested
    class AnnotatedWorkflow {

        @Test
        void usesDefaultPolicyWithoutComponent() {
            // given
            var configurer = WorkflowConfigurer.create();
            configurer.componentRegistry(cr -> cr.registerModule(annotatedModule()));

            // when
            var policy = policyOf(configurer, "annotated-module", "wf-annotated");

            // then
            assertThat(policy).isSameAs(RecoverableWorkflowExceptionPolicy.DEFAULT);
        }

        @Test
        void usesRegisteredComponentPolicy() {
            // given
            var configurer = WorkflowConfigurer.create();
            configurer.componentRegistry(cr -> cr.registerComponent(RecoverableWorkflowExceptionPolicy.class,
                                                                    c -> COMPONENT_POLICY)
                                                 .registerModule(annotatedModule()));

            // when
            var policy = policyOf(configurer, "annotated-module", "wf-annotated");

            // then
            assertThat(policy).isSameAs(COMPONENT_POLICY);
        }
    }
}
