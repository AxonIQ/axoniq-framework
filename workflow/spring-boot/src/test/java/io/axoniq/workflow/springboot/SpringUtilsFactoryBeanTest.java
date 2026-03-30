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
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = SpringUtilsFactoryBeanTest.TestConfig.class)
public class SpringUtilsFactoryBeanTest {

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;

    @Test
    void shouldDetectWorkflowContextFactories() {
        List<SpringUtils.WorkflowContextFactoryBeanDefinition> factoryBeans = SpringUtils.factoryBeans(beanFactory, false);

        assertThat(factoryBeans).hasSize(2);
        assertThat(factoryBeans).extracting(SpringUtils.WorkflowContextFactoryBeanDefinition::beanName)
                                .containsExactlyInAnyOrder("myFactory", "myIndirectFactory");
        assertThat(factoryBeans).extracting(SpringUtils.WorkflowContextFactoryBeanDefinition::workflowContextType)
                                .containsExactlyInAnyOrder(MyWorkflowContext.class, MyWorkflowContext.class);
    }

    @Configuration
    static class TestConfig {
        @Bean
        public MyWorkflowContextFactory myFactory() {
            return new MyWorkflowContextFactory();
        }

        @Bean
        public MyIndirectWorkflowContextFactory myIndirectFactory() {
            return new MyIndirectWorkflowContextFactory();
        }
    }

    static class MyWorkflowContext extends io.axoniq.workflow.dsl.AbstractDSLWorkflowContext {
        public MyWorkflowContext(String workflowId, Map<String, Object> payload, ProcessingContext processingContext, WorkflowConfiguration<?> workflowConfiguration) {
            super(workflowId, payload, processingContext, workflowConfiguration);
        }
    }

    static class MyWorkflowContextFactory implements WorkflowContextFactory<MyWorkflowContext> {
        @Nonnull
        @Override
        public MyWorkflowContext createContext(@Nonnull Map<String, Object> initialPayload, @Nonnull String workflowId, @Nonnull ProcessingContext processingContext, @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            return new MyWorkflowContext(workflowId, initialPayload, processingContext, workflowConfiguration);
        }
    }

    static abstract class BaseWorkflowContextFactory<T extends WorkflowContext> implements WorkflowContextFactory<T> {
    }

    static class MyIndirectWorkflowContextFactory extends BaseWorkflowContextFactory<MyWorkflowContext> {
        @Nonnull
        @Override
        public MyWorkflowContext createContext(@Nonnull Map<String, Object> initialPayload, @Nonnull String workflowId, @Nonnull ProcessingContext processingContext, @Nonnull WorkflowConfiguration<?> workflowConfiguration) {
            return new MyWorkflowContext(workflowId, initialPayload, processingContext, workflowConfiguration);
        }
    }
}
