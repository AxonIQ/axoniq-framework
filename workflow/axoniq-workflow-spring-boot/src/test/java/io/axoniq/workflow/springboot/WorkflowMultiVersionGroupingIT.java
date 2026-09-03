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
package io.axoniq.workflow.springboot;

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
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
 * Verifies that multiple {@link Workflow @Workflow} beans of the same workflow-context type are
 * registered into a SINGLE module that shares one {@link WorkflowConfigurationRegistry} and
 * {@link WorkflowEngine}. This is the architectural prerequisite for cross-version routing: two
 * beans with the same {@code workflowName} but different {@code @Workflow(version=...)} need to be
 * visible to one another so the engine can pick the highest version on start and look up a sibling
 * definition on replay.
 *
 * @author Stefan Dragisic
 */
@SpringBootTest(
        classes = WorkflowMultiVersionGroupingIT.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"axon.axonserver.enabled=false"}
)
public class WorkflowMultiVersionGroupingIT {

    @Autowired
    private WorkflowConfigurationRegistry<?> registry;

    @Test
    void bothBeansShareOneRegistryWithDistinctVersions() {
        var configurations = registry.getWorkflowsConfigurations(new QualifiedName("io.namespace.OrderPlaced"));
        assertThat(configurations)
                .as("Both v1 and v2 @Workflow beans should land in the SAME registry under the same start-event qualified name")
                .hasSize(2);

        var versions = configurations.stream()
                                     .map(c -> c.configuration().workflowVersion())
                                     .sorted()
                                     .toList();
        assertThat(versions).containsExactly("1.0.0", "2.0.0");

        // Highest-version lookup returns only v2.
        var highest = registry.getHighestVersionConfigurations(new QualifiedName("io.namespace.OrderPlaced"));
        assertThat(highest).hasSize(1);
        assertThat(highest.get(0).configuration().workflowVersion()).isEqualTo("2.0.0");

        // Explicit lookup by name + version finds each sibling independently.
        var v1 = registry.findByWorkflowNameAndVersion("OrderWorkflow", "1.0.0");
        var v2 = registry.findByWorkflowNameAndVersion("OrderWorkflow", "2.0.0");
        assertThat(v1).isPresent();
        assertThat(v2).isPresent();
        assertThat(v1.get().workflowVersion()).isEqualTo("1.0.0");
        assertThat(v2.get().workflowVersion()).isEqualTo("2.0.0");
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestConfig {

        @Bean
        public OrderWorkflowV1 orderWorkflowV1() {
            return new OrderWorkflowV1();
        }

        @Bean
        public OrderWorkflowV2 orderWorkflowV2() {
            return new OrderWorkflowV2();
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

    public static class OrderWorkflowV1 {
        @Workflow(
                workflowName = "OrderWorkflow",
                startOnEventName = "io.namespace.OrderPlaced",
                idProperty = "orderId",
                workflowVersion = "1.0.0"
        )
        public void execute(SimpleWorkflowContext ctx) {
        }
    }

    public static class OrderWorkflowV2 {
        @Workflow(
                workflowName = "OrderWorkflow",
                startOnEventName = "io.namespace.OrderPlaced",
                idProperty = "orderId",
                workflowVersion = "2.0.0"
        )
        public void execute(SimpleWorkflowContext ctx) {
        }
    }
}
