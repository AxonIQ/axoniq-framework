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
import io.axoniq.framework.workflow.simulation.scenarios.Inv9TimeoutsFireScenario;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-9 ({@code TimeoutsFire}): a step that exceeds its configured timeout reaches a
 * {@code TIMED_OUT} outcome (recorded) — a timeout never silently hangs or vanishes; the workflow always gets a terminal
 * step outcome it can act on.
 * <p>
 * The first test drives the real engine through {@code TimeoutWorkflow} (a {@code waitForEvent} whose event is never
 * delivered, under a short timeout) and advances virtual time past the timeout window — the clean, fully-virtual
 * wait-timeout path through the injectable {@code WorkflowScheduler} ({@code ManualWorkflowScheduler}): the engine
 * records the step's {@code TIMED_OUT} event, and a crash + replay does not re-arm or re-record the already-timed-out
 * step. The remaining tests are assertion pins proving {@link Invariants#assertTimeoutsFire} is sound and non-trivial: a
 * step that timed out within its window passes, a step still STARTED past its window throws, a step that completed in
 * time passes (any terminal satisfies the property), a step whose window has not yet elapsed is not flagged (no false
 * positive), the check is per {@code (workflowId, stepName)}, and a step with no configured timeout is ignored.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv9TimeoutsFireTest {

    private static final String STEP = "awaitConfirmation";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Map<String, Duration> BOUNDS = Map.of(STEP, TIMEOUT);
    private static final Instant T0 = Instant.EPOCH.plusSeconds(100);

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void timingOutStep_recordsTimedOut_andStaysRecordedAcrossCrash() {
        Inv9TimeoutsFireScenario.Outcome outcome = Inv9TimeoutsFireScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("TimeoutWorkflow must reach a terminal workflow status (its body catches the timeout and ctx.fail)")
                .isTrue();
        // The whole point of INV-9: the timed-out step reached a recorded TIMED_OUT outcome (it did not hang silently).
        assertThat(outcome.stepStatusAtTimeout())
                .as("TimeoutsFire: the step whose wait timeout elapsed must be recorded TIMED_OUT")
                .isEqualTo(StepStatus.TIMED_OUT);
        // INV-9 (in scope): a crash + replay must NOT re-arm/re-record the already-timed-out step.
        assertThat(outcome.stepRecordsAfterCrash())
                .as("TimeoutsFire: the timed-out step's committed records must be unchanged across crash/replay (no "
                            + "fresh wait launched on replay of an already-terminal step)")
                .isEqualTo(outcome.stepRecordsAtTimeout());
    }

    @Test
    void assertTimeoutsFire_passesWhenStepTimedOutWithinWindow() {
        // STARTED then TIMED_OUT: the timeout fired. Even with `now` well past the window, a terminal record satisfies.
        List<EventMessage> log = List.of(
                step("timeout-wf0", STEP, StepStatus.STARTED, T0),
                step("timeout-wf0", STEP, StepStatus.TIMED_OUT, T0.plus(TIMEOUT)));

        assertThatCode(() -> Invariants.assertTimeoutsFire(log, BOUNDS, T0.plusSeconds(3600)))
                .as("a step that recorded TIMED_OUT once its window elapsed satisfies INV-9")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTimeoutsFire_throwsWhenStartedPastWindowWithoutTerminal() {
        // The genuine break: the step went STARTED at T0, its 5s window has long elapsed (now = T0 + 1h), and there is
        // no terminal record — the timeout silently failed to fire. This pins that the assertion catches a hanging step.
        List<EventMessage> log = List.of(
                step("timeout-wf0", STEP, StepStatus.STARTED, T0));

        assertThatThrownBy(() -> Invariants.assertTimeoutsFire(log, BOUNDS, T0.plusSeconds(3600)))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("TimeoutsFire")
                .hasMessageContaining("timeout-wf0/" + STEP);
    }

    @Test
    void assertTimeoutsFire_passesWhenStepCompletedInTime() {
        // The step completed (e.g. its awaited event arrived) before the window elapsed: any terminal status satisfies
        // INV-9 (the property is "did not stay STARTED forever", not "must be TIMED_OUT").
        List<EventMessage> log = List.of(
                step("timeout-wf0", STEP, StepStatus.STARTED, T0),
                step("timeout-wf0", STEP, StepStatus.COMPLETED, T0.plusSeconds(2)));

        assertThatCode(() -> Invariants.assertTimeoutsFire(log, BOUNDS, T0.plusSeconds(3600)))
                .as("a step that COMPLETED in time is not a timeout violation (any terminal outcome satisfies INV-9)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTimeoutsFire_skipsWhenWindowNotYetElapsed() {
        // The step is STARTED and still legitimately waiting: the window has NOT elapsed (now = T0 + 2s < T0 + 5s). This
        // must NOT be flagged — the soundness guard against false positives on a step that is correctly still pending.
        List<EventMessage> log = List.of(
                step("timeout-wf0", STEP, StepStatus.STARTED, T0));

        assertThatCode(() -> Invariants.assertTimeoutsFire(log, BOUNDS, T0.plusSeconds(2)))
                .as("a step still within its timeout window must not be flagged (no false positive)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTimeoutsFire_isPerWorkflowIdAndStep() {
        // Two INDEPENDENT instances: wf0 timed out (fine), wf1 is STARTED but still within its window (fine). Per
        // (workflowId, stepName), neither is a violation — the check must not pool them.
        List<EventMessage> log = List.of(
                step("timeout-wf0", STEP, StepStatus.STARTED, T0),
                step("timeout-wf0", STEP, StepStatus.TIMED_OUT, T0.plus(TIMEOUT)),
                step("timeout-wf1", STEP, StepStatus.STARTED, T0.plusSeconds(3598)));

        assertThatCode(() -> Invariants.assertTimeoutsFire(log, BOUNDS, T0.plusSeconds(3600)))
                .as("the check is per (workflowId, stepName); a timed-out instance and a still-waiting instance both pass")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTimeoutsFire_ignoresStepsWithoutAConfiguredTimeout() {
        // A step with no entry in the bounds map carries no finite timeout (e.g. OrderWorkflow's 365-day wait) and is
        // not constrained by INV-9, even if STARTED long past any small window.
        List<EventMessage> log = List.of(
                step("timeout-wf0", "unbounded", StepStatus.STARTED, T0));

        assertThatCode(() -> Invariants.assertTimeoutsFire(log, BOUNDS, T0.plusSeconds(3600)))
                .as("a step absent from the timeout map (no finite timeout) is not constrained by INV-9")
                .doesNotThrowAnyException();
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status, Instant timestamp) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(UUID.randomUUID().toString(), new MessageType(stepName), Map.of(), metadata,
                                       timestamp);
    }
}
