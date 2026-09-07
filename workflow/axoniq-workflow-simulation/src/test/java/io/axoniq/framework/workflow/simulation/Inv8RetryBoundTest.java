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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.Inv8RetryBoundScenario;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-8 ({@code RetryBound}): for a step configured with {@code RetryPolicy.maxRetries(n)}, the
 * number of attempt records (STARTED/RETRYING) for that {@code (workflowId, stepName)} is at most {@code n + 1}, even
 * across a crash/replay.
 * <p>
 * The first test drives the real engine through {@code RetryingWorkflow} (one always-failing step under
 * {@code maxRetries(k)}): the engine records exactly {@code STARTED + RETRYING×k + FAILED}, so the attempt-record count
 * is exactly {@code k + 1}, and a crash + replay does not launch any further attempt (the terminal step replays as a
 * cached result). The remaining tests are assertion pins proving {@link Invariants#assertRetryBound} is not trivial: a
 * within-bound history passes, an over-bound history throws, the count is per {@code (workflowId, stepName)} (so two
 * instances of the same step do not pool), and a step with no configured bound is ignored.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv8RetryBoundTest {

    private static final String STEP = "flakyShip";

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void alwaysFailingStep_recordsExactlyMaxRetriesPlusOneAttempts_andStaysBoundedAcrossCrash() {
        Inv8RetryBoundScenario.Outcome outcome = Inv8RetryBoundScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("RetryingWorkflow must reach a terminal workflow status (the step failure is absorbed by await())")
                .isTrue();
        // An always-failing step under maxRetries(k) records exactly k+1 attempts: STARTED + RETRYING×k.
        assertThat(outcome.attemptRecordsAtExhaustion())
                .as("RetryBound: an always-failing maxRetries(%d) step records exactly maxRetries+1 attempt records",
                    outcome.maxRetries())
                .isEqualTo(outcome.maxRetries() + 1);
        // INV-8 (in scope): a crash + replay must NOT launch fresh attempts for the already-terminal step.
        assertThat(outcome.attemptRecordsAfterCrash())
                .as("RetryBound: attempt records must stay <= maxRetries+1 across crash/replay (no new attempt on "
                            + "replay of an already-terminal step)")
                .isEqualTo(outcome.attemptRecordsAtExhaustion())
                .isLessThanOrEqualTo(outcome.maxRetries() + 1);
    }

    @Test
    void assertRetryBound_passesForHistoryWithinBound() {
        // maxRetries(2) → bound 3: STARTED + RETRYING + RETRYING + COMPLETED is 3 attempt records — within bound.
        List<EventMessage> log = List.of(
                step("retry-wf0", STEP, StepStatus.STARTED),
                step("retry-wf0", STEP, StepStatus.RETRYING),
                step("retry-wf0", STEP, StepStatus.RETRYING),
                step("retry-wf0", STEP, StepStatus.COMPLETED));

        assertThatCode(() -> Invariants.assertRetryBound(log, Map.of(STEP, 2)))
                .as("3 attempt records for a maxRetries(2) step is within the bound (maxRetries+1)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertRetryBound_throwsWhenAttemptsExceedBound() {
        // The genuine break: maxRetries(2) → bound 3, but the log has 4 attempt records (STARTED + RETRYING×3). This
        // pins that the assertion actually catches an over-bound history (so it isn't trivially satisfied).
        List<EventMessage> log = List.of(
                step("retry-wf0", STEP, StepStatus.STARTED),
                step("retry-wf0", STEP, StepStatus.RETRYING),
                step("retry-wf0", STEP, StepStatus.RETRYING),
                step("retry-wf0", STEP, StepStatus.RETRYING), // one too many — exceeds maxRetries+1 = 3
                step("retry-wf0", STEP, StepStatus.FAILED));

        assertThatThrownBy(() -> Invariants.assertRetryBound(log, Map.of(STEP, 2)))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("RetryBound")
                .hasMessageContaining("retry-wf0/" + STEP);
    }

    @Test
    void assertRetryBound_isPerWorkflowIdAndStep() {
        // Two INDEPENDENT instances each at the bound (3 attempts) must NOT pool into 6 and trip the assertion.
        List<EventMessage> log = List.of(
                step("retry-wf0", STEP, StepStatus.STARTED),
                step("retry-wf1", STEP, StepStatus.STARTED),
                step("retry-wf0", STEP, StepStatus.RETRYING),
                step("retry-wf1", STEP, StepStatus.RETRYING),
                step("retry-wf0", STEP, StepStatus.RETRYING),
                step("retry-wf1", STEP, StepStatus.RETRYING));

        assertThatCode(() -> Invariants.assertRetryBound(log, Map.of(STEP, 2)))
                .as("the bound is per (workflowId, stepName); two instances at the bound must each pass")
                .doesNotThrowAnyException();
    }

    @Test
    void assertRetryBound_ignoresStepsWithoutAConfiguredBound() {
        // A step with no entry in the bounds map carries no retry policy and is not constrained by INV-8, even with
        // many STARTED/RETRYING records (which would never actually happen, but proves the skip is by configuration).
        List<EventMessage> log = List.of(
                step("retry-wf0", "unbounded", StepStatus.STARTED),
                step("retry-wf0", "unbounded", StepStatus.RETRYING),
                step("retry-wf0", "unbounded", StepStatus.RETRYING),
                step("retry-wf0", "unbounded", StepStatus.RETRYING),
                step("retry-wf0", "unbounded", StepStatus.RETRYING));

        assertThatCode(() -> Invariants.assertRetryBound(log, Map.of(STEP, 2)))
                .as("a step absent from the bounds map (no retry policy) is not constrained by INV-8")
                .doesNotThrowAnyException();
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(new MessageType(stepName), Map.of(), metadata);
    }
}
