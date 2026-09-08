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
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.RetryingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Deterministic scenario for INVARIANTS.md INV-8 ({@code RetryBound}): for a step configured with
 * {@code RetryPolicy.maxRetries(n)}, the number of attempt records (STARTED/RETRY_STARTED) for that
 * {@code (workflowId, stepName)} in the committed history is at most {@code n + 1}, even across a crash/replay.
 * <p>
 * Drives {@link RetryingWorkflow}, whose {@code flakyShip} step always fails under {@code maxRetries(k)}. The engine
 * records exactly one {@code STARTED} then {@code RETRYING ×k} then a terminal {@code FAILED} — i.e. exactly
 * {@code k + 1} attempt records — and the workflow then completes (the failure is absorbed by
 * {@code WorkflowStepResult.await()}). The scenario is the implementation twin of the engine's retry-exhaustion path
 * for INV-8, the way {@code TerminalIsFinalScenario} is for INV-7. INV-8 is DST-only (the Phase-2 TLA+ model is scoped
 * to leasing + crash-recovery and has no notion of retries — see INVARIANTS.md INV-8 "Checked by").
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body records {@code reserveInventory}, then drives {@code flakyShip} to retry
 *       exhaustion (STARTED + (RETRYING + RETRY_STARTED)×k + FAILED) and reaches a terminal workflow status;</li>
 *   <li>assert {@link Invariants#assertRetryBound} holds and capture the attempt-record count for {@code flakyShip}
 *       (expected to be exactly {@code k + 1});</li>
 *   <li>crash + recover (drives the real replay path) <strong>alone</strong> — no event is redelivered; the recovered
 *       engine must not launch any further attempt (the recorded terminal {@code flakyShip} replays as a cached result),
 *       so the attempt-record count is byte-for-byte unchanged. This mirrors {@code TerminalIsFinalScenario}'s in-scope
 *       crash+replay check ({@code eventsAfterCrash == eventsAtTerminal}); redelivering the <em>start</em> event is
 *       deliberately avoided because that re-spawns the terminated business key (the documented finding <strong>F-3</strong>),
 *       which would pool a second lifecycle's attempts into the same {@code (workflowId, stepName)} key — out of scope
 *       for INV-8, which constrains a single instance lifecycle's recorded attempts.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class Inv8RetryBoundScenario {

    private Inv8RetryBoundScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal              whether the instance recorded a terminal workflow status.
     * @param maxRetries                   the configured {@code maxRetries} of the flaky step (bound is this + 1).
     * @param attemptRecordsAtExhaustion   number of {@code flakyShip} attempt records (STARTED/RETRY_STARTED) once the step
     *                                     reached retry exhaustion. INV-8 requires this to be {@code ≤ maxRetries + 1};
     *                                     for an always-failing step it is exactly {@code maxRetries + 1}.
     * @param attemptRecordsAfterCrash     number of {@code flakyShip} attempt records after a crash + replay
     *                                     <strong>alone</strong> (no redelivery). INV-8 requires this to be unchanged
     *                                     from {@code attemptRecordsAtExhaustion} (still {@code ≤ maxRetries + 1}) —
     *                                     replay must not launch fresh attempts for the already-terminal step.
     */
    public record Outcome(boolean reachedTerminal, int maxRetries, int attemptRecordsAtExhaustion,
                          int attemptRecordsAfterCrash) {

    }

    /**
     * Runs the scenario against a fresh world (driving {@link RetryingWorkflow}) and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.retryingWorkflow(new CountingEffects());
        var bounds = Map.of(RetryingWorkflow.STEP_FLAKY, RetryingWorkflow.FLAKY_MAX_RETRIES);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "retry-" + orderId;

            // 1. Start the workflow: reserveInventory succeeds, flakyShip retries to exhaustion (FAILED), workflow ends.
            world.engine().publish(new RetryRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "flakyShip to reach retry exhaustion (terminal FAILED)",
                                () -> hasTerminalStep(world.committedLog(), workflowId, RetryingWorkflow.STEP_FLAKY));

            // 2. INV-8 must hold at exhaustion, and the attempt-record count is exactly maxRetries+1 for the
            // always-failing step.
            Invariants.assertRetryBound(world.committedLog(), bounds);
            int attemptsAtExhaustion = attemptRecords(world.committedLog(), workflowId, RetryingWorkflow.STEP_FLAKY);

            // 3. Crash + replay ALONE (no redelivery): the recorded terminal flakyShip must replay as a cached result,
            // launching no fresh attempt — the attempt-record count must be unchanged. (Redelivering the start event
            // would re-spawn the terminated id — finding F-3 — pooling a second lifecycle's attempts; that is out of
            // scope for INV-8, which bounds a single lifecycle, and is already covered by INV-7's F-3 probe.)
            world.crashAndRecover();
            // Give the recovered engine a bounded window to (incorrectly) launch any further attempt during
            // replay/live-switch.
            Polling.await(Duration.ofSeconds(2),
                          () -> attemptRecords(world.committedLog(), workflowId, RetryingWorkflow.STEP_FLAKY)
                                  > attemptsAtExhaustion);
            Invariants.assertRetryBound(world.committedLog(), bounds);
            int attemptsAfterCrash = attemptRecords(world.committedLog(), workflowId, RetryingWorkflow.STEP_FLAKY);

            boolean reachedTerminal = isTerminalWorkflow(world.committedLog(), workflowId);

            return new Outcome(reachedTerminal, RetryingWorkflow.FLAKY_MAX_RETRIES, attemptsAtExhaustion,
                               attemptsAfterCrash);
        }
    }

    /**
     * Counts attempt records (STARTED / RETRY_STARTED step events) for a {@code (workflowId, stepName)}.
     */
    private static int attemptRecords(List<EventMessage> committedLog, String workflowId,
                                      String stepName) {
        return (int) committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())))
                .filter(e -> MetadataUtils.getStepStatus(e.metadata())
                                          .map(s -> s == StepStatus.STARTED || s == StepStatus.RETRY_STARTED)
                                          .orElse(false))
                .count();
    }

    private static boolean hasTerminalStep(List<EventMessage> committedLog, String workflowId,
                                           String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(StepStatus::isTerminal).orElse(false));
    }

    private static boolean isTerminalWorkflow(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }
}
