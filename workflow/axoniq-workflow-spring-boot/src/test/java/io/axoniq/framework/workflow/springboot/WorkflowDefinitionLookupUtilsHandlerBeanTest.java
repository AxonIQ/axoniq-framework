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
package io.axoniq.framework.workflow.springboot;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.AbstractDSLWorkflowContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for detection of workflow definitions in Spring context.
 *
 * @author Simon Zambrovski
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = WorkflowDefinitionLookupUtilsHandlerBeanTest.TestConfig.class)
public class WorkflowDefinitionLookupUtilsHandlerBeanTest {

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;

    @Test
    void shouldDetectWorkflowWorkflowBeanDefinitions() {
        // We look for any workflow context type or a specific one.
        // SpringUtils.handlerBeans takes a Class<? extends WorkflowContext> as a first argument.
        var handlerBeans = WorkflowDefinitionLookupUtils.workflowBeanDefinitions(WorkflowContext.class,
                                                                                 beanFactory,
                                                                                 false);

        assertThat(handlerBeans).hasSize(2);
        assertThat(handlerBeans).containsOnlyKeys(MyWorkflowContext.class, OtherWorkflowContext.class);
        assertThat(handlerBeans.get(MyWorkflowContext.class)).containsExactlyInAnyOrder("myWorkflow");
        assertThat(handlerBeans.get(OtherWorkflowContext.class)).containsExactlyInAnyOrder("myOtherWorkflow");
    }

    @Configuration
    static class TestConfig {

        @Bean
        public MyWorkflow myWorkflow() {
            return new MyWorkflow();
        }

        @Bean
        public MyOtherWorkflow myOtherWorkflow() {
            return new MyOtherWorkflow();
        }

        @Bean
        public NonWorkflowBean nonWorkflowBean() {
            return new NonWorkflowBean();
        }
    }

    static class MyWorkflowContext extends AbstractDSLWorkflowContext {

        public MyWorkflowContext(String workflowId, Map<String, @Nullable Object> payload, ProcessingContext processingContext,
                                 WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    static class OtherWorkflowContext extends AbstractDSLWorkflowContext {

        public OtherWorkflowContext(String workflowId, Map<String, @Nullable Object> payload, ProcessingContext processingContext,
                                    WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    static class MyWorkflow {

        @Workflow(startOnEventName = "StartEvent", idProperty = "id")
        public void define(MyWorkflowContext context) {
        }
    }

    static class MyOtherWorkflow {

        @Workflow(startOnEventName = "OtherStartEvent", idProperty = "id")
        public void define(OtherWorkflowContext context) {
        }
    }

    static class NonWorkflowBean {

        public void someMethod() {
        }
    }
}
