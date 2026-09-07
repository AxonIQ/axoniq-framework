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
import io.axoniq.framework.workflow.simulation.scenarios.TerminalIsFinalScenario;
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
 * Exercises INVARIANTS.md INV-7 ({@code TerminalIsFinal}): once an instance records a terminal workflow status, no
 * further step or status events are ever recorded for it across a crash/replay.
 * <p>
 * The scenario drives the real engine through {@code CancellingWorkflow} (one recorded step, then {@code ctx.cancel()})
 * and crashes + recovers; the instance's committed subsequence must be byte-for-byte unchanged from the moment it first
 * went terminal (replay re-reaches {@code ctx.cancel()} as a no-op). A second test pins finding <strong>F-3</strong>
 * (see {@code formal/POC-TLA-DST.adoc}): redelivering the <em>start</em> event for a terminated business key restarts
 * it — documented engine behaviour, not an INV-7 break, since start-event redelivery is outside the harness's modeled
 * at-least-once set. The remaining pin tests prove the {@link Invariants#assertTerminalIsFinal} assertion itself is
 * correct: it tolerates the cross-instance interleaving of the global log (the F-2 surface, asserted per
 * {@code workflowId}) and a late-appended event of a step that began before terminal — its COMPLETED (fuzz seed 252) or
 * its STARTED (fuzz seed 18) globally appended just after the workflow-completion commit, the F-2 intra-instance
 * append-order artifact — while still catching genuine new work after terminal (a step appearing wholly after terminal,
 * or a second workflow-status event).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv7TerminalIsFinalTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void cancelledWorkflow_recordsNothingAfterTerminal_acrossCrashAndReplay() {
        TerminalIsFinalScenario.Outcome outcome = TerminalIsFinalScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("CancellingWorkflow must reach a terminal (CANCELLED) workflow status")
                .isTrue();
        assertThat(outcome.eventsAtTerminal())
                .as("the instance must have recorded at least its reserveInventory step + the terminal status")
                .isGreaterThanOrEqualTo(2);
        // INV-7 (in scope): a crash + replay alone must NOT append anything for the already-terminal instance.
        assertThat(outcome.eventsAfterCrash())
                .as("TerminalIsFinal: no step/status event may be recorded after the terminal status across crash/replay")
                .isEqualTo(outcome.eventsAtTerminal());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void startEventRedelivery_doesNotRestartTerminatedBusinessKey_F3Closed() {
        // FINDING F-3 (documented expected behaviour, NOT an INV-7 break): redelivering the START event for a business
        // key whose instance already terminated restarts it — the engine evicts terminal instances from the in-memory
        // spawn-dedup repository (WorkflowEngine.switchToLiveMode / on completion) and the dedup
        // (WorkflowSpawnRouting.resolveWorkflowIdForNewSpawn) only consults that in-memory repo, never the durable log,
        // so a fresh STARTED is appended for the same id. See formal/POC-TLA-DST.adoc (F-3). This pins the observed
        // engine behaviour so a future change is a deliberate, reviewed decision; no engine code was changed here.
        TerminalIsFinalScenario.Outcome outcome = TerminalIsFinalScenario.run(0L, "A");
        assertThat(outcome.eventsAfterRedelivery())
                .as("F-3 closed: the redelivered start's ORIGIN-anchored spawn append is rejected, nothing new is "
                            + "appended for the terminated business key")
                .isEqualTo(outcome.eventsAfterCrash());
    }

    @Test
    void assertTerminalIsFinal_toleratesCrossInstanceInterleaving() {
        // Two INDEPENDENT instances: wf0 is terminal (CANCELLED), wf1 records a step AFTER wf0's terminal event in the
        // merged global log. INV-7 is per-instance, so wf1's event is not "after wf0's terminal" for INV-7 — must pass.
        List<EventMessage> log = List.of(
                step("cancel-wf0", "reserveInventory", StepStatus.COMPLETED),
                workflowStatus("cancel-wf0", WorkflowStatus.CANCELLED),
                step("cancel-wf1", "reserveInventory", StepStatus.STARTED));

        assertThatCode(() -> Invariants.assertTerminalIsFinal(log))
                .as("a different instance's event after this one's terminal status is not an INV-7 break")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTerminalIsFinal_detectsStepAfterTerminal() {
        // The genuine break: the SAME instance records a step event after its own terminal workflow status.
        List<EventMessage> log = List.of(
                step("cancel-wf0", "reserveInventory", StepStatus.COMPLETED),
                workflowStatus("cancel-wf0", WorkflowStatus.CANCELLED),
                step("cancel-wf0", "shipOrder", StepStatus.STARTED)); // illegal: after terminal

        assertThatThrownBy(() -> Invariants.assertTerminalIsFinal(log))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("TerminalIsFinal")
                .hasMessageContaining("cancel-wf0");
    }

    @Test
    void assertTerminalIsFinal_toleratesLateTerminalStepRecordAfterTerminal() {
        // The F-2 intra-instance append-order artifact (fuzz seed 252): a step that FINISHED before the workflow
        // completed can have its terminal record globally appended just AFTER the workflow-completion event, because the
        // engine publishes events durably-async. The step's STARTED is present before the terminal status, so this is
        // NOT the engine doing new work after terminal — it must be tolerated. (A genuine break — new work after
        // terminal — is pinned by assertTerminalIsFinal_detectsStepAfterTerminal below; a duplicate STEP terminal record
        // is caught by INV-2, not here; a duplicate WORKFLOW-status terminal record is the F-13 gap, tolerated here and
        // pinned by F13DuplicateCancelRecordTest — INV-2 does NOT count workflow-status terminals.)
        List<EventMessage> log = List.of(
                step("corr-wf0", "recordMatch", StepStatus.STARTED),
                workflowStatus("corr-wf0", WorkflowStatus.COMPLETED),
                step("corr-wf0", "recordMatch", StepStatus.COMPLETED)); // late terminal record after terminal

        assertThatCode(() -> Invariants.assertTerminalIsFinal(log))
                .as("a late-appended terminal step record after the workflow terminal status is tolerated (F-2 "
                            + "intra-instance append order — the engine published it durably-async, not as new work)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTerminalIsFinal_toleratesLateStartedOfPreTerminalStepAfterTerminal() {
        // The F-2 intra-instance append-order artifact, late-STARTED variant (fuzz seed 18, on the pre-existing vmig
        // MigratingOrderWorkflow instance): a step's COMPLETED is committed BEFORE the workflow terminal status but its
        // STARTED is globally appended AFTER it, because the engine publishes events durably-async. The step BEGAN before
        // terminal (its COMPLETED is present earlier), so its late STARTED is the ordering artifact, not new work —
        // tolerated. (A step appearing WHOLLY after terminal — no pre-terminal event — is still flagged as new work; see
        // assertTerminalIsFinal_detectsStepAfterTerminal.)
        List<EventMessage> log = List.of(
                step("vmig-wf0", "processV2", StepStatus.COMPLETED),
                workflowStatus("vmig-wf0", WorkflowStatus.COMPLETED),
                step("vmig-wf0", "processV2", StepStatus.STARTED)); // late STARTED of a step that finished before terminal

        assertThatCode(() -> Invariants.assertTerminalIsFinal(log))
                .as("a late-appended STARTED of a step that began before terminal is tolerated (F-2 intra-instance "
                            + "append order — the engine published it durably-async, not as new work)")
                .doesNotThrowAnyException();
    }

    @Test
    void assertTerminalIsFinal_detectsDifferentTerminalStatusAfterTerminal() {
        // A workflow-status event reaching a DIFFERENT terminal status after the first (the termination changed) is a
        // genuine break (termination must be final and singular in identity). This is NOT the F-13 gap: F-13 is a
        // re-published IDENTICAL terminal status; here the status differs (COMPLETED then FAILED), so it still throws.
        List<EventMessage> log = List.of(
                workflowStatus("cancel-wf0", WorkflowStatus.COMPLETED),
                workflowStatus("cancel-wf0", WorkflowStatus.FAILED)); // illegal: DIFFERENT status after terminal

        assertThatThrownBy(() -> Invariants.assertTerminalIsFinal(log))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("TerminalIsFinal");
    }

    @Test
    void assertTerminalIsFinal_toleratesRepublishedIdenticalTerminalStatus_F13Gap() {
        // FINDING F-13 (documented gap, tolerated): a SECOND, IDENTICAL <workflow>:CANCELLED for an instance already
        // terminal at CANCELLED — the duplicate durable terminal record the engine's ungated cancel-publish path
        // re-publishes when the body re-runs across a crash/replay (intermittent race) or an F-3 start-event redelivery
        // (deterministic restart). Because the re-published status is identical, no NEW lifecycle state is reached, so —
        // exactly like the F-2 late-record tolerance above — it is TOLERATED here while the gap is OPEN (a future
        // F-7-class engine fix gating the publish on "already terminal" flips it back to one record). Pinned end-to-end
        // against the real engine by F13DuplicateCancelRecordTest. Note INV-2 (AtMostOnceRecording) does NOT see this: it
        // counts only step-status terminals, so a duplicate WORKFLOW-status terminal falls solely to INV-7.
        List<EventMessage> log = List.of(
                step("cancel-wf0", "reserveInventory", StepStatus.COMPLETED),
                workflowStatus("cancel-wf0", WorkflowStatus.CANCELLED),
                workflowStatus("cancel-wf0", WorkflowStatus.CANCELLED)); // F-13: re-published IDENTICAL terminal status

        assertThatCode(() -> Invariants.assertTerminalIsFinal(log))
                .as("a re-published identical terminal workflow status is the tolerated F-13 gap (not new work)")
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
