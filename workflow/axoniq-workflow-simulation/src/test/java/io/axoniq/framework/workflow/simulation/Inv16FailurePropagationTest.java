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
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.FailurePropagationScenario;
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
 * Exercises INVARIANTS.md INV-16 ({@code FailurePropagation}): a step failure propagates to a terminal FAILED workflow
 * status — the workflow never silently hangs or completes when a step fails. The failing step is recorded
 * terminally-failed, the instance reaches a terminal FAILED workflow status, and (complementing INV-7) no step after the
 * failing one begins, deterministically across a crash/replay.
 * <p>
 * The first two tests drive the real engine through {@code FailingWorkflow}, on both failure paths: a no-retry uncaught
 * exception (immediate FAILED) and retry exhaustion (FAILED after the policy is exhausted). Each reaches a terminal
 * FAILED workflow status, records its failing step terminally-failed, never runs the post-failure step, and a crash +
 * replay leaves the FAILED terminus byte-for-byte stable (the failed step stays failed, nothing new is appended). The
 * remaining tests are assertion pins proving {@link Invariants#assertFailurePropagation} is not trivial: a terminal
 * step-failure with NO FAILED workflow status (a silent hang) throws; a workflow that COMPLETED despite a failed step (a
 * silent completion) throws; a step recorded after the failing step (no finality) throws; the check is per
 * {@code workflowId} (so a different instance's events do not pool / falsely trip); and an out-of-scope id prefix is
 * skipped.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv16FailurePropagationTest {

    private static final String PREFIX = "fail-";
    private static final String FAILING_STEP = "failingStep";
    private static final String AFTER_STEP = "afterFailure";

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void noRetryFailingStep_propagatesToFailed_andIsStableAcrossCrash() {
        FailurePropagationScenario.Outcome outcome = FailurePropagationScenario.runNoRetry(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("a no-retry failing step must drive the workflow to a terminal status")
                .isTrue();
        // (a) the real-engine run reaches a terminal FAILED workflow status — the failure propagated.
        assertThat(outcome.terminalWorkflowStatus())
                .as("FailurePropagation: a step failure must propagate to a terminal FAILED workflow status (not "
                            + "COMPLETED, not stuck)")
                .isEqualTo(WorkflowStatus.FAILED);
        assertThat(outcome.failingStepTerminallyFailed())
                .as("FailurePropagation: the failing step must be recorded terminally-failed (FAILED/TIMED_OUT)")
                .isTrue();
        assertThat(outcome.afterFailureStepRan())
                .as("FailurePropagation: no step after the failing one may begin (the failure ends the workflow)")
                .isFalse();
        // The FAILED terminus is stable across a crash + replay: nothing new is appended.
        assertThat(outcome.eventsAfterCrash())
                .as("FailurePropagation: the FAILED terminus is stable across crash/replay (no new work after terminal)")
                .isEqualTo(outcome.eventsAtFailure());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void retryExhaustionFailingStep_propagatesToFailed_andIsStableAcrossCrash() {
        FailurePropagationScenario.Outcome outcome = FailurePropagationScenario.runRetryExhaustion(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("a retry-exhausting failing step must drive the workflow to a terminal status")
                .isTrue();
        // (a) the real-engine run reaches a terminal FAILED workflow status after retry exhaustion — the failure
        // propagated (distinct from INV-8, which bounds the attempt-record count on the same shape).
        assertThat(outcome.terminalWorkflowStatus())
                .as("FailurePropagation: an exhausted-retry step failure must propagate to a terminal FAILED status")
                .isEqualTo(WorkflowStatus.FAILED);
        assertThat(outcome.failingStepTerminallyFailed())
                .as("FailurePropagation: the failing step must be recorded terminally-failed after exhaustion")
                .isTrue();
        assertThat(outcome.afterFailureStepRan())
                .as("FailurePropagation: no step after the failing one may begin")
                .isFalse();
        assertThat(outcome.eventsAfterCrash())
                .as("FailurePropagation: the FAILED terminus is stable across crash/replay")
                .isEqualTo(outcome.eventsAtFailure());
    }

    @Test
    void assertFailurePropagation_passesForFailedWorkflowWithFailedStepAndNoPostStep() {
        // The good case: reserveInventory COMPLETED, failingStep FAILED, workflow FAILED, no afterFailure step.
        List<EventMessage> log = List.of(
                step("fail-wf0", "reserveInventory", StepStatus.COMPLETED),
                step("fail-wf0", FAILING_STEP, StepStatus.FAILED),
                workflowStatus("fail-wf0", WorkflowStatus.FAILED));

        assertThatCode(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .as("a FAILED workflow whose failing step is terminally-failed and ran no post-step is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertFailurePropagation_throwsWhenTerminalStepFailureButNoFailedWorkflowStatus() {
        // The silent-failure break: the failing step recorded a terminal FAILURE, but the workflow never recorded a
        // terminal status (it silently hangs non-terminal). A correct engine must propagate to FAILED.
        // Modelled by giving the instance NO terminal workflow status — INV-16 (and the harness liveness check) would
        // not enforce until terminal, so to make the break observable we add a terminal status that is NOT FAILED.
        List<EventMessage> log = List.of(
                step("fail-wf0", FAILING_STEP, StepStatus.FAILED),
                // Reached a terminal status, but TIMED_OUT instead of FAILED — the step failure did not propagate as a
                // FAILED workflow (a silent / wrong-terminus break).
                workflowStatus("fail-wf0", WorkflowStatus.TIMED_OUT));

        assertThatThrownBy(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("FailurePropagation")
                .hasMessageContaining("fail-wf0");
    }

    @Test
    void assertFailurePropagation_throwsWhenWorkflowCompletedDespiteFailedStep() {
        // The silent-completion break: the failing step recorded a terminal FAILURE, yet the workflow COMPLETED — a step
        // failure that surfaced as a completed workflow. INV-16 requires the failure to propagate to FAILED.
        List<EventMessage> log = List.of(
                step("fail-wf0", "reserveInventory", StepStatus.COMPLETED),
                step("fail-wf0", FAILING_STEP, StepStatus.FAILED),
                workflowStatus("fail-wf0", WorkflowStatus.COMPLETED));

        assertThatThrownBy(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("FailurePropagation")
                .hasMessageContaining("COMPLETED");
    }

    @Test
    void assertFailurePropagation_throwsWhenAStepRanAfterTheFailingStep() {
        // The no-finality break: a step after the failing one began even though the workflow reached FAILED.
        List<EventMessage> log = List.of(
                step("fail-wf0", FAILING_STEP, StepStatus.FAILED),
                workflowStatus("fail-wf0", WorkflowStatus.FAILED),
                step("fail-wf0", AFTER_STEP, StepStatus.STARTED)); // illegal: a step after the failing one

        assertThatThrownBy(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("FailurePropagation")
                .hasMessageContaining(AFTER_STEP);
    }

    @Test
    void assertFailurePropagation_throwsWhenFailedWorkflowHasNoTerminalStepFailure() {
        // A FAILED workflow whose failing step has NO terminal failure record (the failure silently vanished from the
        // step). INV-16 requires the failure to be recorded on the step.
        List<EventMessage> log = List.of(
                step("fail-wf0", "reserveInventory", StepStatus.COMPLETED),
                workflowStatus("fail-wf0", WorkflowStatus.FAILED));

        assertThatThrownBy(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("FailurePropagation")
                .hasMessageContaining(FAILING_STEP);
    }

    @Test
    void assertFailurePropagation_toleratesCrossInstanceInterleaving() {
        // (d) Two INDEPENDENT instances: wf0 failed cleanly; wf1 is an unrelated instance whose events interleave in the
        // merged global log. INV-16 is per-instance, so wf1's events must not affect wf0's check — must pass.
        List<EventMessage> log = List.of(
                step("fail-wf0", FAILING_STEP, StepStatus.FAILED),
                step("fail-wf1", "reserveInventory", StepStatus.STARTED),
                workflowStatus("fail-wf0", WorkflowStatus.FAILED),
                step("fail-wf1", FAILING_STEP, StepStatus.FAILED),
                workflowStatus("fail-wf1", WorkflowStatus.FAILED));

        assertThatCode(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .as("the check is per workflowId; two independent failing instances must each pass")
                .doesNotThrowAnyException();
    }

    @Test
    void assertFailurePropagation_skipsOutOfScopePrefix() {
        // (e) An instance with a different id prefix (e.g. a normal order- instance that COMPLETED) is out of INV-16's
        // scope and must be skipped — even though it COMPLETED (which would trip the FAILED-terminus facet if checked).
        List<EventMessage> log = List.of(
                step("order-wf0", "shipOrder", StepStatus.COMPLETED),
                workflowStatus("order-wf0", WorkflowStatus.COMPLETED));

        assertThatCode(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .as("an instance whose id does not start with the failing prefix is not constrained by INV-16")
                .doesNotThrowAnyException();
    }

    @Test
    void assertFailurePropagation_skipsMidFlightInstanceWithNoTerminalStatusYet() {
        // An instance still mid-flight (failing step FAILED but no terminal workflow status yet — the FAILED event has
        // not been committed) is skipped: there is nothing to enforce until the failure has propagated to a terminus.
        List<EventMessage> log = List.of(
                step("fail-wf0", "reserveInventory", StepStatus.COMPLETED),
                step("fail-wf0", FAILING_STEP, StepStatus.FAILED));

        assertThatCode(() -> Invariants.assertFailurePropagation(log, PREFIX, FAILING_STEP, AFTER_STEP))
                .as("a mid-flight instance (no terminal workflow status yet) is skipped until it terminates")
                .doesNotThrowAnyException();
    }

    private static EventMessage step(String workflowId, String stepName, StepStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, stepName, status);
        return new GenericEventMessage(new MessageType(stepName), Map.of(), metadata);
    }

    private static EventMessage workflowStatus(String workflowId, WorkflowStatus status) {
        Metadata metadata = MetadataUtils.create(workflowId, status);
        return new GenericEventMessage(new MessageType("workflow"), Map.of(), metadata);
    }
}
