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
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
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
 * Verifies the external single-step cancellation surface of the {@link WorkflowManager} (issue #224):
 * {@code cancelStep(workflowId, stepName, reason)} authors a durable {@code <step>:CANCELLED} record for a running,
 * non-terminal step while the workflow stays alive (so the body can catch and compensate), and returns whether the
 * step was non-terminal; {@code cancelAllRunningSteps(workflowId, reason)} does the same for every currently-running
 * step and returns the count. Single-step cancel is separate from whole-workflow cancel: it always publishes the
 * per-step terminal event.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class WorkflowManagerStepCancellationTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    public WorkflowManagerStepCancellationTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new CancellableStepWorkflow());
    }

    @Test
    void cancelStep_onRunningStep_returnsTrue_publishesCancelled_bodyCompensates() {
        start("step-ok");
        awaitParked("step-ok");

        var manager = configuration.getComponent(WorkflowManager.class);
        var result = manager.cancelStep("step-ok", "awaitApproval",
                                        CancellationReason.of("operator cancelled the wait"));

        assertThat(result.cancelled()).isTrue();
        assertThat(result.workflowId()).isEqualTo("step-ok");
        assertThat(result.stepName()).isEqualTo("awaitApproval");

        // Body catches the StepCancellationException, compensates, and the workflow completes normally.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById("step-ok");
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.getStep("awaitApproval").status()).isEqualTo(StepStatus.CANCELLED);
            assertThat(state.getStep("compensate").status()).isEqualTo(StepStatus.COMPLETED);
        });
    }

    @Test
    void cancelStep_onAlreadyTerminalStep_returnsFalse() {
        start("step-terminal");
        awaitParked("step-terminal");

        // "prepared" already COMPLETED before the workflow parked.
        var manager = configuration.getComponent(WorkflowManager.class);
        var result = manager.cancelStep("step-terminal", "prepared", CancellationReason.of("too late"));

        assertThat(result.cancelled()).isFalse();

        // The workflow is untouched: still parked, still non-terminal.
        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        assertThat(executionRepository.findById("step-terminal")).isPresent();
        assertThat(executionRepository.findById("step-terminal").orElseThrow().state().workflowStatus())
                .isEqualTo(WorkflowStatus.STARTED);
    }

    @Test
    void cancelStep_onUnknownWorkflow_returnsFalse() {
        var manager = configuration.getComponent(WorkflowManager.class);
        var result = manager.cancelStep("no-such-workflow", "awaitApproval", CancellationReason.none());

        assertThat(result.cancelled()).isFalse();
    }

    @Test
    void cancelAllRunningSteps_cancelsEveryRunningStep_returnsCount() {
        start("step-all");
        awaitParked("step-all");

        var manager = configuration.getComponent(WorkflowManager.class);
        // At this point two steps are running: the async "background" execute and the parked "awaitApproval" wait.
        var result = manager.cancelAllRunningSteps("step-all", CancellationReason.of("cancel all steps"));

        assertThat(result.cancelled()).isEqualTo(2);
        assertThat(result.workflowId()).isEqualTo("step-all");

        // The awaited step's cancellation is caught by the body, which compensates and completes; both running steps
        // record CANCELLED.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById("step-all");
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.getStep("awaitApproval").status()).isEqualTo(StepStatus.CANCELLED);
            assertThat(state.getStep("background").status()).isEqualTo(StepStatus.CANCELLED);
        });
    }

    private void start(@Nonnull String id) {
        delayedPublisher.addSchedules(List.of(ofMillis(100, new StartCancellableEvent(id))));
        delayedPublisher.start();
    }

    private void awaitParked(@Nonnull String workflowId) {
        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById(workflowId);
            assertThat(execution).isPresent();
            var state = execution.get().state();
            assertThat(state.containsStep("prepared")).isTrue();
            assertThat(state.getStep("prepared").status()).isEqualTo(StepStatus.COMPLETED);
            assertThat(state.containsStep("awaitApproval")).isTrue();
            assertThat(state.getStep("awaitApproval").status()).isEqualTo(StepStatus.STARTED);
        });
    }

    /**
     * Runs a step that completes ({@code prepared}), starts a long async step it never awaits ({@code background}),
     * then parks on {@code awaitApproval} and catches its cancellation to compensate via {@code compensate}.
     */
    public static class CancellableStepWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(CancellableStepWorkflow.class);

        @Workflow(
                workflowName = "CancellableStepWorkflow",
                workflowNamespace = "io.axoniq.dsl.managerstepcancel",
                idProperty = "id",
                startOnEventClass = StartCancellableEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            logger.info("CancellableStepWorkflow started for {}", ctx.workflowPayload());
            var id = String.valueOf(ctx.workflowPayload().get("id"));

            ctx.awaitExecute("prepared", Map.of(), (c, p) -> Map.of("prepared", true));

            // Async, never awaited: a long-running background step that stays running while the body parks.
            ctx.execute("background", Map.of(),
                        (c, p) -> {
                            io.axoniq.workflow.runtime.test.utils.SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                            return Map.of("background", true);
                        },
                        step -> step.timeout(Duration.ofMinutes(5)));

            try {
                ctx.awaitEvent("awaitApproval", ApprovalEvent.class,
                               associate(payloadProperty("id"), equalsTo(id)),
                               step -> step.timeout(Duration.ofMinutes(5)));
            } catch (StepCancellationException e) {
                logger.info("awaitApproval cancelled, compensating");
                ctx.awaitExecute("compensate", Map.of(), (c, p) -> Map.of("compensated", true));
            }
        }
    }

    @Event(namespace = "io.axoniq.managerstepcancel", name = "StartCancellable")
    public record StartCancellableEvent(String id) {

    }

    @Event(namespace = "io.axoniq.managerstepcancel", name = "Approval")
    public record ApprovalEvent(String id) {

    }
}
