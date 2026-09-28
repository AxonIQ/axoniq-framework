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
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.test.utils.SleepUtils;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;

/**
 * Issue #224: when a workflow reaches any terminal state while an async step is still running, the engine publishes
 * only the workflow-level terminal event and <b>interrupts</b> the running step (no per-step terminal event); the step
 * is left in its last recorded {@code STARTED} state. Covers all four paths in {@code SimpleWorkflowExecution}: normal
 * completion, {@code ctx.fail(...)}, and {@code ctx.cancel(...)}. (Single-step cancel is the way to get a step terminal
 * + compensation — see {@code CancelStepWorkflowTest}.)
 *
 * @author Stefan Dragisic
 */
class CompleteWithRunningStepWorkflowTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    public CompleteWithRunningStepWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new CompleteWithRunningStepWorkflow());
    }

    @Test
    void runningStepIsInterruptedAndLeftStartedOnCompletion() {
        runAndAssertBackgroundLeftStarted("user-cr-1", "vip", WorkflowStatus.COMPLETED);
    }

    @Test
    void runningStepIsInterruptedAndLeftStartedOnFail() {
        runAndAssertBackgroundLeftStarted("user-cr-2", "FAIL", WorkflowStatus.FAILED);
    }

    @Test
    void runningStepIsInterruptedAndLeftStartedOnExplicitCancel() {
        runAndAssertBackgroundLeftStarted("user-cr-3", "CANCEL", WorkflowStatus.CANCELLED);
    }

    private void runAndAssertBackgroundLeftStarted(String userId, String status,
                                                   WorkflowStatus expectedWorkflowStatus) {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new RegistrationReceivedEvent(userId, userId + "@test.com", status))
        ));
        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == expectedWorkflowStatus);
        testDriver.noExecution();
        testDriver.testingState().hasSteps("background");
        // New semantics (issue #224): whole-workflow terminal interrupts the still-running step (no per-step terminal
        // event) and leaves it in its last recorded STARTED state — it is NOT driven to CANCELLED.
        testDriver.testingState().stepMatches(
                step -> step.stepName().equals("background") && step.status() == StepStatus.STARTED
        );
    }

    public static class CompleteWithRunningStepWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(CompleteWithRunningStepWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.completerunning",
                idProperty = "id",
                startOnEventClass = RegistrationReceivedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            logger.info("completeWithRunningStep workflow started for {}", ctx.workflowPayload());

            ctx.execute(
                    "background",
                    Map.of(),
                    (c, p) -> {
                        SleepUtils.sleepQuietly(Duration.ofMinutes(5));
                        return Map.of("done", true);
                    },
                    step -> step.timeout(Duration.ofMinutes(5))
            );

            var status = payload(ctx.workflowPayload()).<String>get("status");
            switch (status) {
                case "FAIL" -> ctx.fail(new RuntimeException("boom"));
                case "CANCEL" -> ctx.cancel("user-initiated");
                default -> {
                }
            }
        }
    }

    @Event(namespace = "my.custom", name = "RegistrationReceived")
    public record RegistrationReceivedEvent(String id, String email, String status) {

    }
}
