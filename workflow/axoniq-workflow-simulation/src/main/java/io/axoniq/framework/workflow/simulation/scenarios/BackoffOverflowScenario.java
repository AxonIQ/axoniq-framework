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
import io.axoniq.framework.workflow.simulation.workflow.BackoffOverflowWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BackoffOverflowRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Deterministic scenario settling finding <strong>F-9</strong> (S-3): the {@code BackoffStrategy.exponential}
 * shift/{@code Duration} overflow at large attempt counts (an INV-8 {@code RetryBound} / INV-21
 * {@code RetryTimingAndExhaustionEdges} backoff-arithmetic edge) — now <strong>FIXED</strong>. It drives the real engine
 * through {@link BackoffOverflowWorkflow}: a single always-failing {@code execute} step under {@code maxRetries(70)} with
 * {@code BackoffStrategy.exponential(1min, 1s)}.
 * <p>
 * <strong>Was:</strong> {@code BackoffStrategy.exponential(base, max)} computed the delay as
 * {@code factor = 1L << (attempt - 1); computed = base.multipliedBy(factor); return computed > max ? max : computed;} —
 * the cap was applied AFTER the multiply, so for a 1-minute base at {@code attempt == }
 * {@value BackoffOverflowWorkflow#OVERFLOW_ATTEMPT} the factor {@code 2^58} made {@code 60s × 2^58} exceed
 * {@code Duration}'s capacity and {@code multipliedBy} threw {@code ArithmeticException} BEFORE the cap check.
 * {@code RetryableExecuteDelegate.handleAttemptFailure} calls {@code delay(attempt)} (runtime
 * {@code RetryableExecuteDelegate.java:136}) on the workflow thread, so the throw propagated into
 * {@code handleWorkflowException}'s {@code default} branch and WEDGED the instance non-terminally (the F-6/S-4 sink).
 * <p>
 * <strong>Now (fixed):</strong> {@code exponential} clamps to {@code max} before any overflowing/negative/wrapped shift or
 * multiply, so {@code delay(attempt)} returns the capped {@code max} (1s) for every large attempt and never throws. The
 * always-failing step therefore retries to EXHAUSTION on the (clamped) schedule — recording one {@code STARTED} plus
 * {@code maxRetries} {@code RETRYING} records ({@code maxRetries + 1} attempt records, INV-8's bound) — then goes terminal
 * {@code FAILED} and the workflow reaches a terminal status. No wedge, no hang.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class BackoffOverflowScenario {

    private BackoffOverflowScenario() {
    }

    /**
     * Result of running the scenario — the FIXED characterization of finding F-9 (S-3, POC-TLA-DST.adoc).
     *
     * @param overflowAttemptReached whether the failing step's attempt counter reached the formerly-overflowing
     *                               {@value BackoffOverflowWorkflow#OVERFLOW_ATTEMPT}th attempt (the step now sails past
     *                               it on the clamped schedule). OBSERVED: {@code true}.
     * @param reachedTerminal        whether the instance reached a terminal workflow status within the window. OBSERVED:
     *                               {@code true} — the clamped backoff lets the step exhaust and the workflow go terminal.
     * @param hasTerminalStep        whether the failing step recorded a terminal step status (FAILED/TIMED_OUT). OBSERVED:
     *                               {@code true} — the step exhausts its retries and records terminal {@code FAILED}.
     * @param retryingRecords        committed {@code RETRYING} records for the failing step. OBSERVED:
     *                               {@code maxRetries} — one per retry decision, the full bounded schedule.
     * @param attemptRecords         committed attempt records (STARTED + RETRYING) for the failing step. OBSERVED:
     *                               {@code maxRetries + 1} — exactly INV-8's bound (the clamped schedule retries to
     *                               exhaustion without ever exceeding the policy).
     */
    public record Outcome(boolean overflowAttemptReached, boolean reachedTerminal, boolean hasTerminalStep,
                          int retryingRecords, int attemptRecords) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link BackoffOverflowWorkflow} and
     * <strong>characterizes the engine's ACTUAL handling</strong> of the exponential-backoff {@code Duration} overflow.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var registration = EngineInstance.backoffOverflowWorkflow(effects);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "boverflow-" + orderId;

            // 1. Start the failing step. Each retry is scheduled on the virtual scheduler at the (clamped, 1s-capped)
            // backoff delay, so nudge virtual time forward in 1s steps until the failing step's action has run past the
            // formerly-overflowing OVERFLOW_ATTEMPTth attempt — proving the fixed delay() no longer throws there. The
            // step's action bumps the crash-surviving CountingEffects counter on each (re)attempt.
            world.engine().publish(new BackoffOverflowRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(60),
                                "the failing step to sail past the formerly-overflowing attempt ("
                                        + BackoffOverflowWorkflow.OVERFLOW_ATTEMPT + ")",
                                () -> {
                                    world.advanceTime(BackoffOverflowWorkflow.BACKOFF_MAX);
                                    return effects.count(workflowId, BackoffOverflowWorkflow.STEP_OVERFLOW)
                                            >= BackoffOverflowWorkflow.OVERFLOW_ATTEMPT;
                                });
            boolean overflowAttemptReached =
                    effects.count(workflowId, BackoffOverflowWorkflow.STEP_OVERFLOW)
                            >= BackoffOverflowWorkflow.OVERFLOW_ATTEMPT;

            // 2. With the clamped backoff the step exhausts its retries on the bounded schedule and goes terminal FAILED,
            // so the workflow reaches a terminal status. Advance virtual time in 1s steps until that terminal status is
            // committed (it now does — no wedge); the run never hangs.
            Polling.awaitOrFail(Duration.ofSeconds(60),
                                "the always-failing step to exhaust its retries and the workflow to go terminal",
                                () -> {
                                    world.advanceTime(BackoffOverflowWorkflow.BACKOFF_MAX);
                                    return isTerminalWorkflow(world.committedLog(), workflowId);
                                });

            boolean reachedTerminal = isTerminalWorkflow(world.committedLog(), workflowId);
            boolean hasTerminalStep = hasTerminalStep(world.committedLog(), workflowId,
                                                      BackoffOverflowWorkflow.STEP_OVERFLOW);
            int retrying = retryingRecords(world.committedLog(), workflowId, BackoffOverflowWorkflow.STEP_OVERFLOW);
            int attempts = attemptRecords(world.committedLog(), workflowId, BackoffOverflowWorkflow.STEP_OVERFLOW);

            return new Outcome(overflowAttemptReached, reachedTerminal, hasTerminalStep, retrying, attempts);
        }
    }

    // ---- log readers (per (workflowId, stepName), content-based) ----

    private static int retryingRecords(List<EventMessage> committedLog, String workflowId,
                                       String stepName) {
        return (int) committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == StepStatus.RETRYING).orElse(false))
                .count();
    }

    private static int attemptRecords(List<EventMessage> committedLog, String workflowId,
                                      String stepName) {
        return (int) committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> !s.isTerminal()).orElse(false))
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
