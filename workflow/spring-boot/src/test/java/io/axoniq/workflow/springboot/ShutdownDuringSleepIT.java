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

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.test.configuration.PrettyPrintingRecordingEventStore;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reproducer for
 * <a href="https://github.com/AxonIQ/extension-workflow/issues/125">issue #125</a>:
 * the {@code PooledStreamingEventProcessor [WorkflowEngine]} hangs on graceful
 * shutdown while a workflow step is sleeping.
 * <p>
 * Unlike the in-memory declarative tests, this IT drives Spring's
 * {@link ConfigurableApplicationContext#close()} path, which is what the original
 * bug report exercises (Spring Boot graceful shutdown → Axon lifecycle stop →
 * PSEP.stop()).
 */
@SpringBootTest(
        classes = ShutdownDuringSleepIT.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"axon.axonserver.enabled=false"}
)
class ShutdownDuringSleepIT {

    private static final Logger logger = LoggerFactory.getLogger(ShutdownDuringSleepIT.class);

    private static final String SLEEP_STEP = "cooldown";
    private static final Duration SLEEP_DURATION = Duration.ofSeconds(30);
    private static final Duration SHUTDOWN_BUDGET = Duration.ofSeconds(5);

    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private EventSink eventSink;

    @Autowired
    private WorkflowEngine workflowEngine;

    @Test
    void applicationContextCloseShouldNotHangWhenWorkflowIsSleeping() {
        var trigger = new GenericEventMessage(
                new MessageType(new QualifiedName("io.axoniq.issue125.StartSleep")),
                Map.of("id", "user-1")
        );
        eventSink.publish(null, trigger);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            var executions = workflowEngine.workflowExecutions();
            assertThat(executions).hasSize(1);
            var state = executions.iterator().next().state();
            assertThat(state.containsStep(SLEEP_STEP)).isTrue();
            assertThat(state.getStep(SLEEP_STEP).status()).isEqualTo(StepStatus.STARTED);
        });

        var closeFuture = CompletableFuture.runAsync(() -> applicationContext.close());

        assertThat(closeFuture)
                .as("Spring context close should complete within %s even while a %s sleep step is active",
                    SHUTDOWN_BUDGET, SLEEP_DURATION)
                .succeedsWithin(SHUTDOWN_BUDGET);

        var descriptor = new FilesystemStyleComponentDescriptor();
        PrettyPrintingRecordingEventStore.lastInstance().describeTo(descriptor);
        logger.info("Recorded events after shutdown:\n{}", descriptor.describe());
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestConfig {

        @Bean
        public SleepingWorkflow sleepingWorkflow() {
            return new SleepingWorkflow();
        }

        @Bean
        public TokenStore tokenStore() {
            return new InMemoryTokenStore();
        }

        @Bean
        public EventStorageEngine eventStorageEngine() {
            return new InMemoryEventStorageEngine();
        }

        @Bean
        public ConfigurationEnhancer recordingEventStoreEnhancer() {
            return (ComponentRegistry registry) -> registry.registerDecorator(
                    EventStore.class,
                    InterceptingEventStore.DECORATION_ORDER - 1,
                    (cfg, name, delegate) -> PrettyPrintingRecordingEventStore.eventStore(delegate)
            );
        }
    }

    public static class SleepingWorkflow {

        @Workflow(
                workflowName = "ShutdownDuringSleepWorkflow",
                startOnEventName = "io.axoniq.issue125.StartSleep",
                idProperty = "id"
        )
        public void execute(SimpleWorkflowContext ctx) {
            ctx.waitForEvent(SLEEP_STEP, EventConditions.never(), step -> step.timeout(SLEEP_DURATION)).await();
        }
    }
}
