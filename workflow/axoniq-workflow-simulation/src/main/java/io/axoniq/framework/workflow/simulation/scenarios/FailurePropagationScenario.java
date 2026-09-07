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
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance.WorkflowRegistration;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.FailingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FailExhaustionRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FailRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Deterministic scenario for INVARIANTS.md INV-16 ({@code FailurePropagation}): a step failure propagates to a terminal
 * FAILED workflow status — the workflow never silently hangs or completes when a step fails. The failing step is recorded
 * terminally-failed, the instance reaches a terminal FAILED workflow status, and (complementing INV-7) no step after the
 * failing one begins, deterministically across a crash/replay.
 * <p>
 * Drives {@link FailingWorkflow}, whose {@code failingStep} always throws and is awaited via the blocking
 * {@code awaitExecute} so the failure <strong>propagates</strong> (the runtime turns the re-thrown
 * {@code WorkflowFailedException} into the single terminal FAILED workflow-status event). This is the terminal-FAILED twin
 * of {@code TerminalIsFinalScenario} (which drives {@code ctx.cancel}&rarr;CANCELLED for INV-7) and the propagation twin of
 * {@code Inv8RetryBoundScenario} (whose {@code RetryingWorkflow} <em>absorbs</em> its failure to COMPLETED to exercise the
 * INV-8 attempt-count bound). INV-16 is DST-only (the Phase-2 TLA+ model is scoped to leasing + crash-recovery and has no
 * notion of a workflow-level FAILED status — see INVARIANTS.md INV-16 "Checked by").
 * <p>
 * It covers <strong>both</strong> INV-16 failure paths, each in its own single-registration world:
 * <ul>
 *   <li><strong>(a) no-retry uncaught exception</strong> — {@link EngineInstance#failingWorkflowNoRetry}: the failing
 *       step throws once with no retry budget and the workflow goes FAILED immediately;</li>
 *   <li><strong>(b) retry exhaustion</strong> — {@link EngineInstance#failingWorkflowRetryExhaustion}: the failing step
 *       throws on every attempt, exhausts its retry policy, then the workflow goes FAILED.</li>
 * </ul>
 * Steps (per path):
 * <ol>
 *   <li>publish the start event; the body records {@code reserveInventory}, then drives {@code failingStep} to a terminal
 *       failure that propagates — the instance reaches a terminal FAILED workflow status;</li>
 *   <li>assert {@link Invariants#assertFailurePropagation} holds and capture the terminal workflow status, whether the
 *       failing step is recorded terminally-failed, and whether the never-reached {@code afterFailure} step ran;</li>
 *   <li>crash + recover (drives the real replay path) <strong>alone</strong> — no event is redelivered; the recovered
 *       engine re-reaches the already-FAILED instance as a no-op (the recorded terminal {@code failingStep} replays as a
 *       cached result, and {@code ctx} re-throws on the already-terminal instance), so the FAILED terminus is stable: the
 *       failed step stays failed, nothing new is appended, no post-failure step runs (jointly with INV-7/INV-4). This
 *       mirrors {@code TerminalIsFinalScenario}'s in-scope crash+replay check; redelivering the <em>start</em> event is
 *       deliberately avoided because that re-spawns the terminated business key (the documented finding
 *       <strong>F-3</strong>), out of scope here.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FailurePropagationScenario {

    private FailurePropagationScenario() {
    }

    /**
     * Result of running one failure path of the scenario.
     *
     * @param reachedTerminal              whether the instance recorded a terminal workflow status.
     * @param terminalWorkflowStatus       the terminal workflow status reached (INV-16 requires {@code FAILED}).
     * @param failingStepTerminallyFailed  whether the failing step recorded a terminal FAILURE (FAILED/TIMED_OUT) status.
     * @param afterFailureStepRan          whether the never-reached post-failure step ran (INV-16 requires {@code false}).
     * @param eventsAtFailure              number of committed events for the instance once it first went terminal FAILED.
     * @param eventsAfterCrash             number of committed events for the instance after a crash + replay (no
     *                                     redelivery); INV-16 requires this to equal {@code eventsAtFailure} (the FAILED
     *                                     terminus is stable, nothing new appended).
     */
    public record Outcome(boolean reachedTerminal, WorkflowStatus terminalWorkflowStatus,
                          boolean failingStepTerminallyFailed, boolean afterFailureStepRan,
                          int eventsAtFailure, int eventsAfterCrash) {

    }

    /**
     * Runs the <strong>no-retry</strong> failure path (path a): the failing step throws once with no retry budget and the
     * workflow goes FAILED immediately.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome runNoRetry(long seed, String orderId) {
        return run(seed, "fail-" + orderId, EngineInstance.failingWorkflowNoRetry(new CountingEffects()),
                   new FailRequestedEvent(orderId));
    }

    /**
     * Runs the <strong>retry-exhaustion</strong> failure path (path b): the failing step throws on every attempt,
     * exhausts its retry policy, then the workflow goes FAILED.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome runRetryExhaustion(long seed, String orderId) {
        return run(seed, "failretry-" + orderId,
                   EngineInstance.failingWorkflowRetryExhaustion(new CountingEffects()),
                   new FailExhaustionRequestedEvent(orderId));
    }

    /**
     * Shared driver for one failure path: start the failing workflow, wait for it to reach FAILED, assert INV-16 holds,
     * then crash + replay alone and assert the FAILED terminus is stable.
     */
        private static Outcome run(long seed, String workflowId, WorkflowRegistration registration,
                               Object startEvent) {
        try (var world = new SimulationWorld(seed, registration)) {
            String idPrefix = registration.idPrefix();

            // 1. Start the workflow: reserveInventory succeeds, failingStep fails and the failure propagates to FAILED.
            world.engine().publish(startEvent);
            Polling.awaitOrFail(Duration.ofSeconds(10), "failing instance to reach a terminal workflow status",
                                () -> isTerminalWorkflow(world.committedLog(), workflowId));

            // 2. INV-16 must hold at the failure point: failing step recorded terminally-failed, workflow FAILED, no
            // post-failure step ran.
            Invariants.assertFailurePropagation(world.committedLog(), idPrefix, FailingWorkflow.STEP_FAILING,
                                                FailingWorkflow.STEP_AFTER_FAILURE);
            WorkflowStatus terminalStatus = terminalWorkflowStatus(world.committedLog(), workflowId).orElseThrow();
            boolean failingFailed = hasTerminalFailureStep(world.committedLog(), workflowId,
                                                           FailingWorkflow.STEP_FAILING);
            boolean afterRan = hasStep(world.committedLog(), workflowId, FailingWorkflow.STEP_AFTER_FAILURE);
            int eventsAtFailure = instanceEvents(world.committedLog(), workflowId).size();

            // 3. Crash + replay ALONE (no redelivery): the recorded terminal failingStep replays as a cached result and
            // the already-FAILED instance re-reaches the primitive as a no-op, so nothing new is appended — the FAILED
            // terminus is stable (jointly with INV-7/INV-4). (Redelivering the start event would re-spawn the terminated
            // id — finding F-3 — out of scope; already covered by INV-7's F-3 probe.)
            world.crashAndRecover();
            // Give the recovered engine a bounded window to (incorrectly) append anything during replay/live-switch.
            Polling.await(Duration.ofSeconds(2),
                          () -> instanceEvents(world.committedLog(), workflowId).size() > eventsAtFailure);
            Invariants.assertFailurePropagation(world.committedLog(), idPrefix, FailingWorkflow.STEP_FAILING,
                                                FailingWorkflow.STEP_AFTER_FAILURE);
            int eventsAfterCrash = instanceEvents(world.committedLog(), workflowId).size();

            return new Outcome(true, terminalStatus, failingFailed, afterRan, eventsAtFailure, eventsAfterCrash);
        }
    }

    private static boolean isTerminalWorkflow(List<EventMessage> committedLog, String workflowId) {
        return terminalWorkflowStatus(committedLog, workflowId).isPresent();
    }

        private static Optional<WorkflowStatus> terminalWorkflowStatus(List<EventMessage> committedLog,
                                                                   String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .map(e -> MetadataUtils.getWorkflowStatus(e.metadata()).orElse(null))
                           .filter(s -> s != null && s.isTerminal())
                           .findFirst();
    }

    private static boolean hasTerminalFailureStep(List<EventMessage> committedLog, String workflowId,
                                                  String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata())
                                        .map(s -> s == StepStatus.FAILED || s == StepStatus.TIMED_OUT)
                                        .orElse(false));
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent()
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }

        private static List<EventMessage> instanceEvents(List<EventMessage> committedLog,
                                                     String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .toList();
    }
}
