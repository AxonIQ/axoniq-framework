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
package io.axoniq.workflow.itest;

import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.management.WorkflowManager;
import io.axoniq.workflow.runtime.api.management.WorkflowManager.CancellationReason;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.Associations.associate;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the external {@link WorkflowManager} cancellation API (issues #195 / #21): a live workflow parked on
 * {@code awaitEvent} can be cancelled by id or by state predicate from a thread other than its own control thread, and
 * is driven to a durable {@link WorkflowStatus#CANCELLED} terminal state. Non-matching instances are left untouched.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
class ExternalWorkflowCancellationTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    public ExternalWorkflowCancellationTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new ParkedWorkflow());
    }

    @Test
    void cancelById_drivesParkedWorkflowToDurableCancelled() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartParkedEvent("cancel-1", "EU"))
        ));
        delayedPublisher.start();

        awaitParked("cancel-1");

        var manager = configuration.getComponent(WorkflowManager.class);
        boolean cancelled = manager.workflow("cancel-1")
                                   .cancel(CancellationReason.of("operator cancelled by id"));

        assertThat(cancelled).isTrue();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById("cancel-1");
            assertThat(history).isPresent();
            assertThat(history.get().state().workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
        });
    }

    @Test
    void cancelByPredicate_cancelsOnlyMatchingInstance() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartParkedEvent("region-eu", "EU")),
                ofMillis(150, new StartParkedEvent("region-us", "US"))
        ));
        delayedPublisher.start();

        awaitParked("region-eu");
        awaitParked("region-us");

        var manager = configuration.getComponent(WorkflowManager.class);
        var result = manager.workflows(state -> "EU".equals(state.payload().get("region")))
                            .cancel(CancellationReason.of("cancel EU region"));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.affected()).isEqualTo(1);
        assertThat(result.workflowIds()).containsExactly("region-eu");

        // The EU instance becomes durably CANCELLED; the US instance stays non-terminal and running.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var eu = workflowHistoryRepository.findById("region-eu");
            assertThat(eu).isPresent();
            assertThat(eu.get().state().workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
        });

        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        assertThat(executionRepository.findById("region-us")).isPresent();
        assertThat(executionRepository.findById("region-us").orElseThrow().state().workflowStatus())
                .isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void engineCancelByPredicate_cancelsOnlyMatchingInstance() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(100, new StartParkedEvent("engine-eu", "EU")),
                ofMillis(150, new StartParkedEvent("engine-us", "US"))
        ));
        delayedPublisher.start();

        awaitParked("engine-eu");
        awaitParked("engine-us");

        var result = workflowEngine.cancel(state -> "EU".equals(state.payload().get("region")));

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.workflowIds()).containsExactly("engine-eu");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var eu = workflowHistoryRepository.findById("engine-eu");
            assertThat(eu).isPresent();
            assertThat(eu.get().state().workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
        });

        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        assertThat(executionRepository.findById("engine-us")).isPresent();
    }

    private void awaitParked(@Nonnull String workflowId) {
        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById(workflowId);
            assertThat(execution).isPresent();
            var state = execution.get().state();
            assertThat(state.containsStep("awaitSignal")).isTrue();
            assertThat(state.getStep("awaitSignal").status()).isEqualTo(StepStatus.STARTED);
        });
    }

    /**
     * A workflow that parks indefinitely on {@code awaitSignal}. It deliberately does not catch the cancellation, so an
     * external cancel must be what drives it terminal.
     */
    public static class ParkedWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(ParkedWorkflow.class);

        @Workflow(
                workflowName = "ParkedWorkflow",
                workflowNamespace = "io.axoniq.dsl.externalwfcancel",
                idProperty = "id",
                startOnEventClass = StartParkedEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            logger.info("ParkedWorkflow started for {}", ctx.workflowPayload());
            var id = String.valueOf(ctx.workflowPayload().get("id"));
            ctx.awaitEvent(
                    "awaitSignal",
                    SignalEvent.class,
                    associate(payloadProperty("id"), equalsTo(id)),
                    step -> step.timeout(Duration.ofMinutes(5))
            );
            // Only reached if the signal actually arrives (it never does in these tests).
            ctx.awaitExecute("afterSignal", Map.of(), (c, p) -> Map.of("done", true));
        }
    }

    @Event(namespace = "io.axoniq.externalwfcancel", name = "StartParked")
    public record StartParkedEvent(String id, String region) {

    }

    @Event(namespace = "io.axoniq.externalwfcancel", name = "Signal")
    public record SignalEvent(String id) {

    }
}
