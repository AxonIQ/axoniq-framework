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
package io.axoniq.framework.workflow.simulation.scenarios;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DuplicatePayloadRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Deterministic scenario for <strong>F-7</strong> (now <strong>FIXED</strong>): {@code PayloadDelegate.modifyPayload}
 * gated its publish on {@code !containsStep(stepName)} (the replay-skip gate every other state-publishing primitive has),
 * so a post-crash live re-run no longer re-publishes a <strong>duplicate</strong> {@code <step>:COMPLETED} — INVARIANTS.md
 * INV-2 ({@code AtMostOnceRecording}) now <em>holds</em> for {@code modifyPayload} too.
 * <p>
 * It drives {@link DuplicatePayloadWorkflow} (registered as a single definition via
 * {@link EngineInstance#duplicatePayloadWorkflow}): a {@code modifyPayload} step that is NOT the last step, followed by a
 * never-arriving {@code waitForEvent} that keeps the instance LIVE / non-terminal. This opens the exact crash window F-7
 * needed — after {@code finalizePayload}'s {@code COMPLETED} commits but before any {@code <workflow>:COMPLETED} (there is
 * none; the instance is parked on the wait) — and confirms the fix holds across it.
 * <p>
 * The symmetry the fix restores (confirmed in the engine source):
 * <ul>
 *   <li>{@code ExecuteDelegate.execute} gates its STARTED/COMPLETED publish on {@code !state().containsStep(stepName)}
 *       ({@code ExecuteDelegate.java:117}) and routes its COMPLETED through the guarded
 *       {@code AbstractStepExecutor.sendStepEvent}, which refuses to publish an already-terminal step
 *       ({@code AbstractStepExecutor.java:204-212}). A post-crash live re-run therefore <em>skips</em> the re-publish.</li>
 *   <li>{@code PayloadDelegate.modifyPayload} now likewise gates BOTH the drift guard AND its publish task on
 *       {@code !containsStep(stepName)} ({@code PayloadDelegate.java:83}) — so on a post-crash live re-run, with the step
 *       already present, it skips re-publishing. Previously it gated only the drift guard, appended its publish task
 *       unconditionally, and published the {@code COMPLETED} directly via {@code eventSink.publish} (bypassing the
 *       terminal-step guard), re-publishing a SECOND {@code <step>:COMPLETED} — the F-7 duplicate this fix removes.</li>
 * </ul>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs the {@code modifyPayload} step (its {@code finalizePayload:COMPLETED}
 *       commits durably) and parks on the never-arriving wait — the instance is LIVE, no {@code <workflow>:COMPLETED};</li>
 *   <li>count the {@code finalizePayload} terminal records in the committed log — exactly 1 (the per-{@code (workflowId,
 *       stepName)} count, content-based / F-2-robust: never the global append order);</li>
 *   <li>crash + recover (the ordinary replay path), then confirm the {@code finalizePayload:COMPLETED} count stays
 *       <strong>1</strong> — the re-run skips the re-publish (INV-2 holds, the F-7 fix).</li>
 * </ol>
 * The verdict observable is the per-{@code (workflowId, stepName)} terminal-record count, content-based and F-2-robust:
 * it counts terminal records for the {@code finalizePayload} step name within this instance's own committed events, never
 * relying on the (non-deterministic) global append order of one instance's events.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DuplicateTerminalPayloadRecordScenario {

    private DuplicateTerminalPayloadRecordScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param terminalRecordsBeforeCrash the number of committed {@code finalizePayload} terminal records before the crash
     *                                   (the single {@code modifyPayload} COMPLETED — must be 1).
     * @param terminalRecordsAfterRecover the number of committed {@code finalizePayload} terminal records after the crash
     *                                    + recover + live re-run (with the F-7 fix: stays 1 — the re-run skips the
     *                                    re-publish).
     */
    public record Outcome(int terminalRecordsBeforeCrash, int terminalRecordsAfterRecover) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link DuplicatePayloadWorkflow} and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.duplicatePayloadWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "duppay-" + orderId;

            // 1. Start the workflow: the body runs the modifyPayload step (finalizePayload:COMPLETED commits) and parks on
            // the never-arriving wait, staying LIVE / non-terminal (no <workflow>:COMPLETED).
            world.engine().publish(new DuplicatePayloadRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10),
                                "modifyPayload finalizePayload:COMPLETED to commit",
                                () -> terminalRecordCount(world.committedLog(), workflowId) >= 1);

            int beforeCrash = terminalRecordCount(world.committedLog(), workflowId);

            // 2. Crash + recover: replay rebuilds finalizePayload as COMPLETED (present in state), switches to live, and
            // re-runs the body for the still-non-terminal instance — re-reaching the modifyPayload call with the step
            // already present. With the F-7 fix, PayloadDelegate now gates its publish task on !containsStep, so the
            // re-run SKIPS the re-publish — the finalizePayload:COMPLETED count stays 1 (INV-2 holds).
            world.crashAndRecover();
            // Give the re-run a bounded window to (incorrectly) re-publish a duplicate before reading the count back —
            // it must not. The await is a non-failing settle; the duplicate would never appear with the fix in place.
            Polling.await(Duration.ofSeconds(5),
                          () -> terminalRecordCount(world.committedLog(), workflowId) >= 2);

            int afterRecover = terminalRecordCount(world.committedLog(), workflowId);
            return new Outcome(beforeCrash, afterRecover);
        }
    }

    /**
     * Counts the committed terminal step records for {@code finalizePayload} within {@code workflowId}'s own events
     * (content-based, per-{@code (workflowId, stepName)}, F-2-robust — never the global append order).
     */
    private static int terminalRecordCount(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getStepStatus(e.metadata())
                                                           .map(StepStatus::isTerminal).orElse(false))
                                 .filter(e -> DuplicatePayloadWorkflow.STEP_FINALIZE_PAYLOAD
                                         .equals(MetadataUtils.getStepName(e.metadata())))
                                 .count();
    }
}
