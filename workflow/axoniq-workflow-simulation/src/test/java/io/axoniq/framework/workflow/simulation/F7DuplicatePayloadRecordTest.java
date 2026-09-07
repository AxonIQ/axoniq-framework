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
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.DuplicateTerminalPayloadRecordScenario;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DuplicatePayloadRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Pins finding <strong>F-7</strong> (now <strong>FIXED</strong>): {@code PayloadDelegate.modifyPayload} now gates its
 * publish on {@code !containsStep(stepName)} (the replay-skip gate every other state-publishing primitive has), so a
 * post-crash live re-run no longer re-publishes a <strong>duplicate</strong> {@code <step>:COMPLETED} — INVARIANTS.md
 * INV-2 ({@code AtMostOnceRecording}) now <em>holds</em> for {@code modifyPayload} too.
 * <p>
 * The scenario drives the real engine through {@link DuplicatePayloadWorkflow}: a {@code modifyPayload} step (NOT the
 * last step) records and durably commits its {@code finalizePayload:COMPLETED}, then the instance parks on a
 * never-arriving {@code waitForEvent} (LIVE / non-terminal, no {@code <workflow>:COMPLETED}). A crash + recover replays
 * the committed {@code COMPLETED}, switches to live, and re-runs the body for the still-non-terminal instance —
 * re-reaching the {@code modifyPayload} call with the step already present; with the fix, {@code PayloadDelegate} sees
 * the step is present and <em>skips</em> appending its publish task, so the {@code finalizePayload:COMPLETED} count stays
 * <strong>1</strong> — one terminal record for one {@code (workflowId, stepName)}.
 * <p>
 * Mirroring how {@code F0EffectDuplicationTest} / {@code Inv17StatusHookFiresOncePerStatusTest} were flipped from
 * documenting a gap to asserting the fixed property, this test now asserts the F-7 gap is <strong>closed</strong> (the
 * count stays 1 across the crash/recover) and that {@link Invariants#assertAtMostOnceRecording} does <em>not</em> trip on
 * the recovered log. The complementary assertions remain as guards: before the crash INV-2 holds (exactly one terminal
 * record), and the same crash-window scenario built on an {@code execute} step (always-guarded) likewise stays at 1 —
 * confirming {@code modifyPayload} now matches the other primitives' replay-skip behaviour rather than diverging from it.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class F7DuplicatePayloadRecordTest {

    private static final String STEP = DuplicatePayloadWorkflow.STEP_FINALIZE_PAYLOAD;

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void modifyPayload_doesNotRepublishTerminalRecordOnPostCrashLiveReRun_f7Fixed() {
        DuplicateTerminalPayloadRecordScenario.Outcome outcome =
                DuplicateTerminalPayloadRecordScenario.run(0L, "A");

        // Before the crash: the single modifyPayload COMPLETED — INV-2 holds (exactly one terminal record).
        assertThat(outcome.terminalRecordsBeforeCrash())
                .as("before the crash, finalizePayload has exactly one terminal record (INV-2 holds)")
                .isEqualTo(1);

        // F-7 FIXED: the post-crash live re-run finds the step already present and SKIPS the re-publish, so the
        // finalizePayload:COMPLETED count stays 1 — one terminal record for one step (INV-2 holds across crash/recover).
        assertThat(outcome.terminalRecordsAfterRecover())
                .as("F-7 fixed: modifyPayload gates its publish on !containsStep, so the post-crash live re-run does NOT "
                            + "re-publish a duplicate finalizePayload:COMPLETED (the count stays 1)")
                .isEqualTo(1);
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void assertAtMostOnceRecording_holdsAcrossCrashRecover_f7Fixed() {
        // Drive the same window directly so the assertion runs against the engine's real committed log (rather than the
        // scenario's distilled count): before the crash INV-2 holds; after the crash + recover it STILL holds (the fix).
        var registration = EngineInstance.duplicatePayloadWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(0L, registration)) {
            String workflowId = "duppay-A";

            world.engine().publish(new DuplicatePayloadRequestedEvent("A"));
            Polling.awaitOrFail(Duration.ofSeconds(10), "modifyPayload finalizePayload:COMPLETED to commit",
                                () -> terminalCount(world.committedLog(), workflowId, STEP) >= 1);

            // Before the crash: INV-2 (AtMostOnceRecording) holds for this instance.
            assertThatCode(() -> Invariants.assertAtMostOnceRecording(world.committedLog()))
                    .as("before the crash, AtMostOnceRecording holds (one terminal record)")
                    .doesNotThrowAnyException();

            world.crashAndRecover();
            // Give the re-run a bounded settle window to (incorrectly) re-publish a duplicate — it must not (the fix).
            Polling.await(Duration.ofSeconds(5),
                          () -> terminalCount(world.committedLog(), workflowId, STEP) >= 2);

            // After the crash + recover: the re-run skipped the re-publish, so the count stays 1 and INV-2 holds (F-7 fix).
            assertThat(terminalCount(world.committedLog(), workflowId, STEP))
                    .as("F-7 fixed: no second finalizePayload:COMPLETED was committed on the post-crash live re-run")
                    .isEqualTo(1);
            assertThatCode(() -> Invariants.assertAtMostOnceRecording(world.committedLog()))
                    .as("F-7 fixed: AtMostOnceRecording (INV-2) holds — modifyPayload no longer re-records its terminal "
                                + "step across a crash/recover")
                    .doesNotThrowAnyException();
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void contrast_executeStepDoesNotDuplicateInTheSameWindow_matchesTheFixedModifyPayload() {
        // Same crash window built on an execute step (then a never-arriving wait) instead of modifyPayload: ExecuteDelegate
        // has always gated its publish on !containsStep and routed COMPLETED through the guarded sendStepEvent, so the
        // post-crash live re-run does NOT re-publish — the terminal record stays 1. With the F-7 fix, modifyPayload now
        // behaves identically (see the tests above), so this confirms the fixed modifyPayload matches the other primitives'
        // replay-skip behaviour rather than diverging from it.
        var registration = new EngineInstance.WorkflowRegistration(
                "ExecuteContrastWorkflow", DuplicatePayloadRequestedEvent.class, "duppay-",
                F7DuplicatePayloadRecordTest::executeContrastBody);
        try (var world = new SimulationWorld(0L, registration)) {
            String workflowId = "duppay-A";

            world.engine().publish(new DuplicatePayloadRequestedEvent("A"));
            Polling.awaitOrFail(Duration.ofSeconds(10), "execute finalizePayload:COMPLETED to commit",
                                () -> terminalCount(world.committedLog(), workflowId, STEP) >= 1);
            int before = terminalCount(world.committedLog(), workflowId, STEP);

            world.crashAndRecover();
            // Give the re-run the SAME bounded window the modifyPayload case used to (potentially) re-publish.
            Polling.await(Duration.ofSeconds(5),
                          () -> terminalCount(world.committedLog(), workflowId, STEP) >= 2);
            int after = terminalCount(world.committedLog(), workflowId, STEP);

            assertThat(before)
                    .as("execute: one terminal record before the crash")
                    .isEqualTo(1);
            assertThat(after)
                    .as("contrast: an execute step does NOT re-publish its COMPLETED on the post-crash live re-run "
                                + "(ExecuteDelegate gates on !containsStep + routes through the guarded sendStepEvent) — "
                                + "the fixed modifyPayload now matches this replay-skip behaviour")
                    .isEqualTo(1);
            // INV-2 still holds for the execute contrast — no duplicate terminal record.
            assertThatCode(() -> Invariants.assertAtMostOnceRecording(world.committedLog()))
                    .as("execute contrast: AtMostOnceRecording holds (no duplicate)")
                    .doesNotThrowAnyException();
        }
    }

    /**
     * The execute contrast body: an {@code execute} step named exactly {@link #STEP} (so the terminal-record count
     * targets the same step name as the {@code modifyPayload} case), then a never-arriving {@code waitForEvent} keeping
     * the instance LIVE — structurally identical to {@link DuplicatePayloadWorkflow} except the recorded-terminal step is
     * an {@code execute} rather than a {@code modifyPayload}.
     */
    private static void executeContrastBody(io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext ctx) {
        Object orderId = ctx.workflowPayload().get("orderId");
        ctx.awaitExecute(STEP, Map.of(), (pc, payload) -> Map.of("finalized", true));
        ctx.awaitEvent(
                DuplicatePayloadWorkflow.STEP_AWAIT_SIGNAL,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DuplicatePayloadSignalEvent.class,
                associate(payloadProperty("orderId"), equalsTo(orderId)),
                step -> step.timeout(Duration.ofDays(365)));
    }

    private static int terminalCount(List<EventMessage> log, String workflowId, String step) {
        return (int) log.stream()
                        .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                        .filter(e -> MetadataUtils.getStepStatus(e.metadata())
                                                  .map(StepStatus::isTerminal).orElse(false))
                        .filter(e -> step.equals(MetadataUtils.getStepName(e.metadata())))
                        .count();
    }
}
