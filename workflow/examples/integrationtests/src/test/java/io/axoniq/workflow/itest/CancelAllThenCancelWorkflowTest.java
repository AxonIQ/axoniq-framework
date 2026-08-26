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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.test.AbstractWorkflowTestBase;
import io.axoniq.workflow.runtime.test.utils.SleepUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that joining {@code requestRunningStepCancellations(workflowId, reason)} immediately followed by joining
 * {@code requestWorkflowCancellation(workflowId, reason)}, called back to back on the same external thread, deterministically
 * cancels the directly-awaited {@code awaitedStep} with a durable {@code CANCELLED} record, runs the body's
 * compensation, and drives the workflow to a terminal {@code CANCELLED} state.
 * <p>
 * The determinism holds by construction: joining the first future blocks the caller until its control-thread task has
 * fully finished, every durable {@code <step>:CANCELLED} record included. The subsequent workflow cancellation
 * request is therefore not issued until the per-step cancellations are complete. The body observes
 * {@code awaitedStep}'s cancellation as a catchable {@link StepCancellationException}, compensates, then parks on
 * {@code holdStep} (which never completes on its own) until the external workflow-cancellation request terminates it.
 *
 * @author Stefan Dragisic
 */
class CancelAllThenCancelWorkflowTest extends AbstractWorkflowTestBase<SimpleWorkflowContext> {

    private static final Logger logger = LoggerFactory.getLogger(CancelAllThenCancelWorkflowTest.class);

    /** Set true iff the body caught StepCancellationException around awaitedStep and compensated. */
    static final AtomicBoolean COMPENSATION_RAN = new AtomicBoolean(false);

    public CancelAllThenCancelWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new TwoRunningStepsWorkflow());
    }

    @BeforeEach
    void resetMarkers() {
        COMPENSATION_RAN.set(false);
    }

    @Test
    void cancelAllRunningStepsThenCancelBackToBackCancelsAwaitedStepAndCompensates() {
        assertCancellationSequence("cancel-all-then-cancel");
    }

    /**
     * Runs the probabilistic scheduling stress check outside the normal build.
     *
     * Enable with {@code -Dworkflow.stress-tests=true}.
     *
     * @param repetitionInfo information about the current stress-test repetition
     */
    @Tag("stress")
    @EnabledIfSystemProperty(named = "workflow.stress-tests", matches = "true")
    @RepeatedTest(20)
    void cancelAllRunningStepsThenCancelStressTest(RepetitionInfo repetitionInfo) {
        assertCancellationSequence("cancel-all-then-cancel-" + repetitionInfo.getCurrentRepetition());
    }

    private void assertCancellationSequence(@Nonnull String id) {
        delayedPublisher.addSchedules(List.of(ofMillis(100, new StartTwoRunningStepsEvent(id))));
        delayedPublisher.start();

        awaitParked(id);
        workflowCancellationService.requestCancellationOfAllSteps(
                id, new StepCancellationException("cancel all running steps")
        ).orTimeout(10, TimeUnit.SECONDS).join();
        workflowCancellationService.requestWorkflowCancellation(id, new WorkflowCancelledException("cancel workflow"))
                                   .orTimeout(10, TimeUnit.SECONDS)
                                   .join();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById(id);
            assertThat(history).isPresent();
            var state = history.get().state();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
            assertThat(state.getStep("awaitedStep").status()).isEqualTo(StepStatus.CANCELLED);
            assertThat(state.getStep("backgroundStep").status()).isEqualTo(StepStatus.CANCELLED);
        });

        assertThat(COMPENSATION_RAN.get())
                .as("the body must observe awaitedStep's StepCancellationException and compensate")
                .isTrue();

        logger.info("compensation ran, workflow {} CANCELLED", id);
    }

    private void awaitParked(@Nonnull String workflowId) {
        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById(workflowId);
            assertThat(execution).isPresent();
            var state = execution.get().state();
            assertThat(state.containsStep("backgroundStep")).isTrue();
            assertThat(state.getStep("backgroundStep").status()).isEqualTo(StepStatus.STARTED);
            assertThat(state.containsStep("awaitedStep")).isTrue();
            assertThat(state.getStep("awaitedStep").status()).isEqualTo(StepStatus.STARTED);
        });
    }

    /**
     * Starts a long, never-awaited {@code backgroundStep}, parks the body on {@code awaitedStep} inside a try/catch
     * that compensates when the wait is cancelled, then parks on {@code holdStep} (which never completes on its own)
     * so only the external whole-workflow cancel can terminate the instance.
     */
    public static class TwoRunningStepsWorkflow {

        private static final Logger log = LoggerFactory.getLogger(TwoRunningStepsWorkflow.class);

        @Workflow(
                workflowName = "TwoRunningStepsWorkflow",
                workflowNamespace = "io.axoniq.dsl.cancelallthencancel",
                idProperty = "id",
                startOnEventClass = StartTwoRunningStepsEvent.class
        )
        public void execute(@Nonnull SimpleWorkflowContext ctx) {
            log.info("TwoRunningStepsWorkflow started for {}", ctx.workflowPayload());

            ctx.execute("backgroundStep", Map.of(),
                        (c, p) -> {
                            SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                            return Map.of();
                        },
                        step -> step.timeout(Duration.ofMinutes(5)));

            try {
                ctx.awaitExecute("awaitedStep", Map.of(),
                                 (c, p) -> {
                                     SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                                     return Map.of();
                                 },
                                 step -> step.timeout(Duration.ofMinutes(5)));
            } catch (StepCancellationException e) {
                log.info("awaitedStep cancelled -> compensating");
                COMPENSATION_RAN.set(true);
            }

            // Park until the external whole-workflow cancel terminates the instance; the unwind it raises here
            // propagates and is benign once the workflow is already terminal.
            ctx.awaitExecute("holdStep", Map.of(),
                             (c, p) -> {
                                 SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                                 return Map.of();
                             },
                             step -> step.timeout(Duration.ofMinutes(5)));
        }
    }

    @Event(namespace = "io.axoniq.cancelallthencancel", name = "StartTwoRunningSteps")
    public record StartTwoRunningStepsEvent(String id) {

    }
}
