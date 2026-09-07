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
import io.axoniq.framework.workflow.simulation.invariants.Invariants.RetryStepSpec;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OnRetryFires;
import io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryEdgesRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryTimeoutRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Deterministic scenario for INVARIANTS.md INV-21 ({@code RetryTimingAndExhaustionEdges}): the retry/timeout edges INV-8
 * ({@code RetryBound}) and INV-9 ({@code TimeoutsFire}) did not cover — backoff-strategy timing reconstructed from the
 * recorded {@code RETRYING} timestamps (survives crash/replay), a {@code retryWhile} predicate bounding the attempt
 * records, the {@code onRetry} handler firing once per actual retry decision and NOT re-firing on crash/replay (the F-0
 * analogue for retry handlers), and a per-attempt {@code execute} timeout reaching a terminal {@code TIMED_OUT}.
 * <p>
 * Two halves, each on its own fresh world (so the per-attempt {@code execute}-timeout edge — which rides the
 * non-injectable {@code orTimeout} residual, exactly like INV-9's wait timeout — is driven in isolation):
 * <ol>
 *   <li><strong>retry edges</strong> ({@link RetryTimingWorkflow#executeRetryEdges}, ids {@code retryedge-}): start the
 *       instance, advance virtual time to let each backoff-delayed retry fire (the fixed/linear/exponential steps each
 *       fail twice then succeed; the {@code retryWhile} step always fails but its predicate stops it after one retry),
 *       drive to a terminal workflow status, then assert {@link Invariants#assertRetryTimingAndExhaustionEdges} (the
 *       backoff/exhaustion edges) and {@link Invariants#assertOnRetryFiredOncePerRetry} (onRetry fired exactly once per
 *       committed {@code RETRYING}). Then crash + replay <strong>alone</strong> (no redelivery): the recorded-terminal
 *       steps replay as cached results and the engine resumes a {@code RETRYING} step from its persisted state WITHOUT
 *       re-invoking {@code onRetry}, so the crash-surviving {@code onRetry} fire counts are unchanged — the headline
 *       no-re-fire probe (if a count grew, that is a real finding).</li>
 *   <li><strong>per-attempt execute timeout</strong> ({@link RetryTimingWorkflow#executeTimeoutEdge}, ids
 *       {@code retrytimeout-}): start the instance; its slow {@code execute} blocks on a never-released latch, so the
 *       per-attempt {@code timeout} window must elapse. Drive virtual time (the lock-step {@code MutableClock}) past each
 *       attempt's per-attempt window so the engine's {@code ExecuteDelegate} {@code orTimeout} branch fires and records a
 *       {@code TIMED_OUT} outcome; with {@code maxRetries(n)} the total budget is {@code (retries + 1) × timeout} and the
 *       step ultimately reaches a terminal {@code TIMED_OUT} — it never hangs.</li>
 * </ol>
 * Like INV-9/16/17/18 it is <strong>scenario-pinned</strong> (deterministic scenario/test only, not in the per-step fuzz
 * set): the per-attempt {@code execute} timeout rides the {@code orTimeout} residual (Phase-3 D5), and a
 * deliberately-retrying/timing-out always-on fuzz instance would complicate the F-0 effect-count documentation and the
 * liveness horizon check.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class RetryTimingAndExhaustionEdgesScenario {

    private RetryTimingAndExhaustionEdgesScenario() {
    }

    /**
     * Result of the retry-edges half.
     *
     * @param reachedTerminal      whether the instance recorded a terminal workflow status.
     * @param fixedRetrying        committed {@code RETRYING} records for the fixed-backoff step (expected
     *                             {@link RetryTimingWorkflow#BACKOFF_MAX_RETRIES}).
     * @param linearRetrying       committed {@code RETRYING} records for the linear-backoff step.
     * @param exponentialRetrying  committed {@code RETRYING} records for the exponential-backoff step.
     * @param retryWhileRetrying   committed {@code RETRYING} records for the {@code retryWhile} step (expected strictly
     *                             fewer than {@link RetryTimingWorkflow#RETRY_WHILE_MAX_RETRIES}).
     * @param fixedOnRetryBefore   {@code onRetry} fires for the fixed step at exhaustion, before the crash.
     * @param fixedOnRetryAfter    {@code onRetry} fires for the fixed step after a crash + replay — must be unchanged
     *                             (no re-fire).
     * @param retryWhileOnRetry    {@code onRetry} fires for the {@code retryWhile} step (must equal
     *                             {@code retryWhileRetrying}).
     */
    public record RetryEdgesOutcome(boolean reachedTerminal, int fixedRetrying, int linearRetrying,
                                    int exponentialRetrying, int retryWhileRetrying, int fixedOnRetryBefore,
                                    int fixedOnRetryAfter, int retryWhileOnRetry) {

    }

    /**
     * Result of the per-attempt-execute-timeout half.
     *
     * @param reachedTerminal     whether the instance recorded a terminal workflow status.
     * @param slowStepStatus      the recorded terminal status of the slow {@code execute} step (expected
     *                            {@code TIMED_OUT}).
     * @param slowAttemptRecords  committed attempt records (STARTED/RETRYING) for the slow step (bounded by
     *                            {@code maxRetries + 1}).
     */
    public record TimeoutEdgeOutcome(boolean reachedTerminal, StepStatus slowStepStatus,
                                     int slowAttemptRecords) {

    }

    /**
     * The expected retry-step shapes the scenario and the always-on assertion pass to
     * {@link Invariants#assertRetryTimingAndExhaustionEdges}.
     *
     * @return the per-step specs for the retry-edges workflow.
     */
        public static Map<String, RetryStepSpec> retryEdgeSpecs() {
        return Map.of(
                RetryTimingWorkflow.STEP_FIXED,
                new RetryStepSpec(RetryTimingWorkflow.BACKOFF_MAX_RETRIES, RetryTimingWorkflow.BACKOFF_MAX_RETRIES, false),
                RetryTimingWorkflow.STEP_LINEAR,
                new RetryStepSpec(RetryTimingWorkflow.BACKOFF_MAX_RETRIES, RetryTimingWorkflow.BACKOFF_MAX_RETRIES, true),
                RetryTimingWorkflow.STEP_EXPONENTIAL,
                new RetryStepSpec(RetryTimingWorkflow.BACKOFF_MAX_RETRIES, RetryTimingWorkflow.BACKOFF_MAX_RETRIES, true),
                RetryTimingWorkflow.STEP_RETRY_WHILE,
                new RetryStepSpec(RetryTimingWorkflow.RETRY_WHILE_MAX_RETRIES,
                                  RetryTimingWorkflow.RETRY_WHILE_STOP_ATTEMPT - 1, false));
    }

    /**
     * Runs the retry-edges half: backoff timing across crash/replay, {@code retryWhile} bound, and the onRetry-not-
     * re-fired probe.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static RetryEdgesOutcome runRetryEdges(long seed, String orderId) {
        var effects = new CountingEffects();
        var onRetryFires = new OnRetryFires();
        var registration = EngineInstance.retryTimingRetryEdgesWorkflow(effects, onRetryFires);
        var specs = retryEdgeSpecs();
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "retryedge-" + orderId;

            // 1. Start the workflow. Each backoff step fails twice then succeeds; the retries are scheduled on the virtual
            // scheduler at the strategy delay, so nudge virtual time forward until the instance terminates. (The largest
            // single backoff is the exponential cap; advancing in steps comfortably past it lets every retry fire.)
            world.engine().publish(new RetryEdgesRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(30), "the retry-edges instance to reach a terminal workflow status",
                                () -> {
                                    // Drive the backoff timers: advance virtual time so each scheduled retry becomes due.
                                    world.advanceTime(Duration.ofSeconds(1));
                                    return isTerminalWorkflow(world.committedLog(), workflowId);
                                });

            // 2. INV-21 backoff/exhaustion edges hold, and onRetry fired exactly once per committed RETRYING.
            Invariants.assertRetryTimingAndExhaustionEdges(world.committedLog(), "retryedge-", specs);
            int fixedRetrying = retryingRecords(world.committedLog(), workflowId, RetryTimingWorkflow.STEP_FIXED);
            int linearRetrying = retryingRecords(world.committedLog(), workflowId, RetryTimingWorkflow.STEP_LINEAR);
            int expRetrying = retryingRecords(world.committedLog(), workflowId, RetryTimingWorkflow.STEP_EXPONENTIAL);
            int retryWhileRetrying = retryingRecords(world.committedLog(), workflowId,
                                                     RetryTimingWorkflow.STEP_RETRY_WHILE);
            Invariants.assertOnRetryFiredOncePerRetry(workflowId, RetryTimingWorkflow.STEP_FIXED,
                                                      onRetryFires.count(workflowId, RetryTimingWorkflow.STEP_FIXED),
                                                      fixedRetrying);
            Invariants.assertOnRetryFiredOncePerRetry(workflowId, RetryTimingWorkflow.STEP_RETRY_WHILE,
                                                      onRetryFires.count(workflowId, RetryTimingWorkflow.STEP_RETRY_WHILE),
                                                      retryWhileRetrying);
            int fixedOnRetryBefore = onRetryFires.count(workflowId, RetryTimingWorkflow.STEP_FIXED);
            int retryWhileOnRetry = onRetryFires.count(workflowId, RetryTimingWorkflow.STEP_RETRY_WHILE);

            // 3. Crash + replay ALONE (no redelivery): the recorded-terminal retry steps replay as cached results and a
            // resumed RETRYING step is re-scheduled from its persisted attempt WITHOUT re-invoking onRetry — so the
            // crash-surviving onRetry fire count must be unchanged. A grown count is the F-0-analogue re-fire (a finding).
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2),
                          () -> onRetryFires.count(workflowId, RetryTimingWorkflow.STEP_FIXED) > fixedOnRetryBefore);
            Invariants.assertRetryTimingAndExhaustionEdges(world.committedLog(), "retryedge-", specs);
            int fixedOnRetryAfter = onRetryFires.count(workflowId, RetryTimingWorkflow.STEP_FIXED);
            // The recorded RETRYING count must also be unchanged across replay (no fresh attempt on an already-terminal
            // step), so the post-crash onRetry check uses the same expected count.
            Invariants.assertOnRetryFiredOncePerRetry(workflowId, RetryTimingWorkflow.STEP_FIXED, fixedOnRetryAfter,
                                                      retryingRecords(world.committedLog(), workflowId,
                                                                      RetryTimingWorkflow.STEP_FIXED));

            boolean reachedTerminal = isTerminalWorkflow(world.committedLog(), workflowId);
            return new RetryEdgesOutcome(reachedTerminal, fixedRetrying, linearRetrying, expRetrying, retryWhileRetrying,
                                         fixedOnRetryBefore, fixedOnRetryAfter, retryWhileOnRetry);
        }
    }

    /**
     * Runs the per-attempt-execute-timeout half: the slow {@code execute} blocks past its per-attempt {@code timeout},
     * and driving virtual time past each attempt's window must record a terminal {@code TIMED_OUT}.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static TimeoutEdgeOutcome runTimeoutEdge(long seed, String orderId) {
        var effects = new CountingEffects();
        var onRetryFires = new OnRetryFires();
        // Never released: the slow execute action blocks on it so the per-attempt orTimeout window deterministically
        // elapses. CountDownLatch.await() responds to thread interruption, so the engine's shutdown can still unwind it.
        var latch = new CountDownLatch(1);
        var registration = EngineInstance.retryTimingTimeoutEdgeWorkflow(effects, onRetryFires, latch);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "retrytimeout-" + orderId;

            // PRE-ADVANCE the lock-step virtual clock so the slow step's SHORT per-attempt timeout window is ALREADY
            // elapsed by the time the step records STARTED and ExecuteDelegate computes its remainingTimeout. The engine
            // computes remainingTimeout = recordedStartedTimestamp + EXECUTE_TIMEOUT - Instant.now(clock); the recorded
            // STARTED timestamp comes from the in-memory event store's GenericEventMessage clock (the un-injectable static
            // wall clock — the D5 residual, ~system time), while the harness MutableClock starts at the Unix epoch. So we
            // advance the MutableClock to (current system wall time + EXECUTE_TIMEOUT + a generous buffer): then for the
            // slow step ExecuteDelegate's remainingTimeout is negative, and it records the per-attempt TIMED_OUT
            // IMMEDIATELY on the next task cycle (ExecuteDelegate.java line ~160, the negative-remaining branch) — no real
            // wall-clock waiting on the orTimeout JDK timer. reserveSlot's long (365-day) timeout means it is never
            // affected by this pre-advance. With maxRetries(SLOW_EXECUTE_MAX_RETRIES) the retry attempts also start within
            // the buffer, so each likewise computes a negative remaining and times out, until the step goes terminal
            // TIMED_OUT after the (retries+1)×timeout budget.
            Duration preAdvance = Duration.between(Instant.EPOCH, Instant.now())
                                          .plus(RetryTimingWorkflow.EXECUTE_TIMEOUT)
                                          .plus(Duration.ofMinutes(5));
            world.advanceTime(preAdvance);

            // 1. Start the workflow: reserveSlot succeeds (long timeout), slowExecuteCall records STARTED then blocks on
            // the latch while its per-attempt window is already elapsed.
            world.engine().publish(new RetryTimeoutRequestedEvent(orderId));

            // 2. The slow step must reach a terminal (TIMED_OUT) step record via the per-attempt timeout, never hanging.
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "slowExecuteCall to reach a terminal (TIMED_OUT) step status via the per-attempt timeout",
                                () -> hasTerminalStep(world.committedLog(), workflowId,
                                                      RetryTimingWorkflow.STEP_SLOW_EXECUTE));

            StepStatus slowStatus = latestStepStatus(world.committedLog(), workflowId,
                                                     RetryTimingWorkflow.STEP_SLOW_EXECUTE).orElseThrow();
            int slowAttempts = attemptRecords(world.committedLog(), workflowId, RetryTimingWorkflow.STEP_SLOW_EXECUTE);
            // INV-8's attempt bound still holds on the timeout path: at most maxRetries + 1 attempt records.
            Invariants.assertRetryBound(world.committedLog(),
                                        Map.of(RetryTimingWorkflow.STEP_SLOW_EXECUTE,
                                               RetryTimingWorkflow.SLOW_EXECUTE_MAX_RETRIES));
            // 3. The WORKFLOW itself must then reach a terminal status (the "never hangs" guarantee) — AWAIT it rather
            // than reading eagerly: the workflow-level terminal status is committed on a task cycle AFTER the slow step's
            // TIMED_OUT step record, so reading without awaiting races under CPU load (the workflow-terminal status can
            // lag the step-terminal record by a cycle). Mirrors runRetryEdges's terminal-workflow await above.
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the timeout-edge instance to reach a terminal workflow status (never hangs)",
                                () -> isTerminalWorkflow(world.committedLog(), workflowId));
            boolean reachedTerminal = isTerminalWorkflow(world.committedLog(), workflowId);
            return new TimeoutEdgeOutcome(reachedTerminal, slowStatus, slowAttempts);
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

        private static Optional<StepStatus> latestStepStatus(List<EventMessage> committedLog,
                                                         String workflowId, String stepName) {
        StepStatus latest = null;
        for (EventMessage event : committedLog) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                    && stepName.equals(MetadataUtils.getStepName(event.metadata()))) {
                var status = MetadataUtils.getStepStatus(event.metadata());
                if (status.isPresent()) {
                    latest = status.get();
                }
            }
        }
        return Optional.ofNullable(latest);
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
