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

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Integration test for workflow autodetection.
 *
 * @author Simon Zambrovski
 */
@SpringBootTest(
        classes = WorkflowAutodetectionIT.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"axon.axonserver.enabled=false"}
)
public class WorkflowAutodetectionIT {

    @Autowired
    private WorkflowConfigurationRegistry<?> registry;

    @Autowired
    private TestWorkflow testWorkflow;

    @Test
    void autodetectsWorkflow() {

        var configurations = registry.getWorkflowsConfigurations(new QualifiedName("io.namespace.TestEvent"));
        assertThat(configurations).isNotNull();
        assertThat(configurations).hasSize(1);
        var conf = configurations.getFirst();
        assertThat(conf.predicate()
                       .test(
                               new GenericEventMessage(
                                       new MessageType(new QualifiedName("io.namespace.TestEvent")),
                                       null
                               ),
                               mock()
                       )
        ).isTrue();
        assertThat(conf.configuration().workflowContextFactory()).isInstanceOf(SimpleWorkflowContextFactory.class);

        var ctx = mock(SimpleWorkflowContext.class);
        @SuppressWarnings("unchecked")
        WorkflowConfiguration<SimpleWorkflowContext> workflowConfiguration =
                (WorkflowConfiguration<SimpleWorkflowContext>) conf.configuration();
        workflowConfiguration.workflowDefinition().accept(ctx);

        assertThat(testWorkflow.executed).isTrue();
    }


    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestConfig {

        @Bean
        public TestWorkflow testWorkflow() {
            return new TestWorkflow();
        }

        @Bean
        public TokenStore tokenStore() {
            return new InMemoryTokenStore();
        }

        @Bean
        public EventStorageEngine eventStorageEngine() {
            return new InMemoryEventStorageEngine();
        }
    }


    public static class TestWorkflow {

        private boolean executed = false;

        @Workflow(
                workflowName = "TestWorkflow",
                startOnEventName = "io.namespace.TestEvent"
        )
        public void test(SimpleWorkflowContext context) {
            executed = true;
        }
    }
}
