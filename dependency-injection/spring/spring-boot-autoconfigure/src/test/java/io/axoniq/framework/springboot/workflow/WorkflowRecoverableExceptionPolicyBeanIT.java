/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.workflow;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.QualifiedName;
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

/**
 * Tests that a {@link RecoverableWorkflowExceptionPolicy} bean reaches the configuration of an annotated workflow.
 *
 * @author Stefan Dragisic
 */
@SpringBootTest(
        classes = WorkflowRecoverableExceptionPolicyBeanIT.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "axon.axonserver.enabled=false",
                "axon.postgresql.enabled=false"
        }
)
class WorkflowRecoverableExceptionPolicyBeanIT {

    private static final RecoverableWorkflowExceptionPolicy BEAN_POLICY = e -> e instanceof IllegalStateException;

    @Autowired
    private Configuration configuration;

    @Test
    void annotatedWorkflowUsesThePolicyBean() {
        // when
        WorkflowConfigurationRegistry<?> registry =
                configuration.getComponents(WorkflowConfigurationRegistry.class)
                             .get("WorkflowConfigurationRegistry[SimpleWorkflowContext]");
        var configurations = registry.getWorkflowsConfigurations(new QualifiedName("io.namespace.PolicyTestEvent"));

        // then
        assertThat(configurations).hasSize(1);
        assertThat(configurations.getFirst().configuration().recoverableExceptionPolicy()).isSameAs(BEAN_POLICY);
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestConfig {

        @Bean
        public RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy() {
            return BEAN_POLICY;
        }

        @Bean
        public PolicyTestWorkflow policyTestWorkflow() {
            return new PolicyTestWorkflow();
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

    public static class PolicyTestWorkflow {

        @Workflow(workflowName = "PolicyTestWorkflow", startOnEventName = "io.namespace.PolicyTestEvent")
        public void run(SimpleWorkflowContext context) {
        }
    }
}
