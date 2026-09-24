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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowCancelledException;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.dsl.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.test.utils.SleepUtils;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that when a whole-workflow {@code cancel()} interrupts a step the body is parked on, the body's blocking
 * wait unblocks with a catchable {@link StepInterruptedException} (a {@link StepFailedException}), so it can run
 * compensation, even though no durable {@code <step>:CANCELLED} record is ever published for the interrupted step.
 *
 * @author Stefan Dragisic
 */
class CancelWorkflowInterruptsStepWithCatchableExceptionTest
        extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    /**
     * Set true iff the body actually caught StepInterruptedException around the parked step and ran compensation.
     */
    static final AtomicBoolean COMPENSATION_RAN = new AtomicBoolean(false);
    private static final Logger logger =
            LoggerFactory.getLogger(CancelWorkflowInterruptsStepWithCatchableExceptionTest.class);
    /**
     * Records the throwable class that actually unwound the body's await, for the assertion below.
     */
    static volatile String BODY_EXIT = "none";

    public CancelWorkflowInterruptsStepWithCatchableExceptionTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @BeforeEach
    void resetMarkers() {
        COMPENSATION_RAN.set(false);
        BODY_EXIT = "none";
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d.autodetected(c -> new ParkedStepWorkflow());
    }

    @Test
    void cancelWorkflowDeliversCatchableExceptionToParkedStep() {
        var id = "interrupt-catch-1";
        delayedPublisher.addSchedules(List.of(ofMillis(100, new StartParkedStepEvent(id))));
        delayedPublisher.start();

        awaitParked(id);

        workflowCancellationService.requestWorkflowCancellation(
                id, new WorkflowCancelledException("operator cancelled while step running")
        ).join();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var history = workflowHistoryRepository.findById(id).join();
            assertThat(history).isPresent();
            assertThat(history.get().state().workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED);
        });

        await().atMost(Duration.ofSeconds(10))
               .untilAsserted(() -> assertThat(COMPENSATION_RAN.get())
                       .as("body should have caught StepInterruptedException and compensated")
                       .isTrue());

        assertThat(BODY_EXIT).isEqualTo(StepInterruptedException.class.getSimpleName());

        // Whole-workflow termination publishes no per-step terminal event: stepA stays at its last recorded
        // (STARTED) state, even though the body observed a catchable exception in-process.
        var history = workflowHistoryRepository.findById(id).join().orElseThrow();
        assertThat(history.state().getStep("stepA").status()).isEqualTo(StepStatus.STARTED);
    }

    private void awaitParked(String workflowId) {
        var executionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = executionRepository.findById(workflowId);
            assertThat(execution).isPresent();
            var state = execution.get().state();
            assertThat(state.containsStep("stepA")).isTrue();
            assertThat(state.getStep("stepA").status()).isEqualTo(StepStatus.STARTED);
        });
    }

    /**
     * Parks the body on a long-running {@code stepA}, awaited via the blocking convenience, inside a try/catch that
     * marks compensation when the wait is interrupted by whole-workflow termination.
     */
    public static class ParkedStepWorkflow {

        private static final Logger log = LoggerFactory.getLogger(ParkedStepWorkflow.class);

        @Workflow(
                workflowName = "ParkedStepWorkflow",
                workflowNamespace = "io.axoniq.dsl.cancelinterruptscatch",
                idProperty = "id",
                startOnEventClass = StartParkedStepEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            log.info("ParkedStepWorkflow started for {}", ctx.workflowPayload());

            try {
                ctx.awaitExecute("stepA", Map.of(),
                                 (c, p) -> {
                                     SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                                     return Map.of("stepA", true);
                                 },
                                 step -> step.timeout(Duration.ofMinutes(5)));
            } catch (StepInterruptedException e) {
                log.info("stepA wait interrupted by whole-workflow termination -> compensating");
                BODY_EXIT = e.getClass().getSimpleName();
                COMPENSATION_RAN.set(true);
            }
        }
    }

    @Event(namespace = "io.axoniq.cancelinterruptscatch", name = "StartParkedStep")
    public record StartParkedStepEvent(String id) {

    }
}
