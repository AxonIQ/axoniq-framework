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
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedWaitRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.LoopCounterPollRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.LoopReusedPollRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PollSignalEvent;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Phase-4 production-realism scenarios: the documented <strong>retry-loop</strong> recipe under its two authorings,
 * and a <strong>broadcast storm</strong> over many waiters sharing one correlation key.
 * <p>
 * The headline probe characterizes the loop with <strong>reused step names</strong> — the authoring the canonical
 * {@code PaymentWorkflow} example actually ships ({@code "paymentPrepared"}/{@code "retryPayment"} verbatim every
 * iteration). Step names are durable dedup keys, so once iteration 1 records the wait's TIMED_OUT and the sleep's
 * terminal, every later iteration reads those cached results instantly: the retry loop degenerates to a hot,
 * record-less spin that never re-registers the wait (blind to any fresh signal) — invisible in the event log and to
 * any log-based liveness check.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class LoopAndStormScenario {

    /**
     * Bounded wall-clock deadline for poll steps.
     */
    private static final Duration DEADLINE = Duration.ofSeconds(15);

    /**
     * Per-nudge virtual-time advance flushing any straggler timer (the sleep, when the era gap leaves it positive).
     */
    private static final Duration NUDGE = Duration.ofSeconds(5);

    private LoopAndStormScenario() {
    }

    /**
     * Outcome of the reused-names live-lock probe.
     *
     * @param bodyIterations     how many times the loop body ran (the {@code loopIteration} effect counter) —
     *                           {@code MAX_SPINS} under the gap: every iteration after the first is an instant cached
     *                           read.
     * @param pollStartedRecords committed STARTED records for the (single, reused) poll step — 1: the wait registered
     *                           once, in iteration 1, and NEVER re-registered.
     * @param pollTerminalRecords committed terminal records for the poll step — 1 (the iteration-1 TIMED_OUT).
     * @param retryDelayRecords  committed records (any status) for the reused sleep step.
     * @param retryDelayTerminalRecords committed TERMINAL records for the reused sleep step — 2 under the INV-2 gap
     *                           this probe surfaced: the loop's instant re-entry of the reused-name timed wait
     *                           re-publishes a duplicate TIMED_OUT (the publish is gated only on the IN-MEMORY step
     *                           status, which lags the first TIMED_OUT's durably-async apply).
     * @param terminalStatus     the workflow's terminal status — FAILED via the body's own exhaustion guard (a
     *                           production body without the bound would spin forever, non-terminal and invisible).
     */
    public record ReusedNamesOutcome(int bodyIterations, int pollStartedRecords, int pollTerminalRecords,
                                     int retryDelayRecords, int retryDelayTerminalRecords,
                                     @Nullable WorkflowStatus terminalStatus) {

    }

    /**
     * Outcome of the counter-names contrast probe.
     *
     * @param bodyIterations how many times the loop body ran — 1 (the signal arrived during iteration 1's park).
     * @param terminalStatus the workflow's terminal status — COMPLETED.
     * @param processEffects {@code processSignal} effect executions — 1.
     */
    public record CounterNamesOutcome(int bodyIterations, @Nullable WorkflowStatus terminalStatus,
                                      int processEffects) {

    }

    /**
     * Outcome of the aligned-clock multi-iteration pacing pin.
     *
     * @param bodyIterations            how many times the loop body ran (the {@code loopIteration} effect counter) —
     *                                  3: two genuinely timed-out iterations plus the signalled third.
     * @param poll1TimedOutRecords      committed TIMED_OUT records for {@code pollSignal#1} — exactly 1.
     * @param poll2TimedOutRecords      committed TIMED_OUT records for {@code pollSignal#2} — exactly 1.
     * @param poll3CompletedRecords     committed COMPLETED records for {@code pollSignal#3} — exactly 1 (the signal).
     * @param sleep1TerminalRecords     committed terminal records for {@code retryDelay#1} — exactly 1 (the 2s sleep
     *                                  genuinely paced iteration 1 under the aligned clock).
     * @param sleep2TerminalRecords     committed terminal records for {@code retryDelay#2} — exactly 1.
     * @param processEffects            {@code processSignal} effect executions — 1.
     * @param terminalStatus            the workflow's terminal status — COMPLETED.
     */
    public record AlignedPacingOutcome(int bodyIterations, int poll1TimedOutRecords, int poll2TimedOutRecords,
                                       int poll3CompletedRecords, int sleep1TerminalRecords, int sleep2TerminalRecords,
                                       int processEffects, @Nullable WorkflowStatus terminalStatus) {

    }

    /**
     * Outcome of the broadcast-storm probe.
     *
     * @param completedInstances how many of the waiters reached COMPLETED after the single broadcast signal.
     * @param totalWaiters       how many waiters were parked on the shared key.
     * @param perInstanceMatches per-instance {@code recordMatch} effect counts summed — must equal
     *                           {@code totalWaiters} (each woke exactly once).
     */
    public record BroadcastOutcome(int completedInstances, int totalWaiters, int perInstanceMatches) {

    }

    /**
     * The reused-names live-lock probe: start the loop, let iteration 1's poll time out (era-anchored advance) and its
     * sleep resolve, then observe iterations 2..{@code MAX_SPINS} spin through cached results instantly — no new
     * records, no re-registration — until the body's own bound fails the instance.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code loopr-<orderId>}).
     * @return the observed outcome.
     */
        public static ReusedNamesOutcome reusedNamesLiveLock(long seed, String orderId) {
        return reusedNamesLiveLock(seed, orderId, null);
    }

    /**
     * Phase-2 overload: same probe with an optional body-executor override, so the deterministic-carrier /
     * seeded-interleaving executor can drive the F-20 re-entry race as a seeded, reproducible schedule dimension.
     *
     * @param seed                 seed for the world's deterministic id source.
     * @param orderId              business key for the single instance.
     * @param bodyExecutorOverride optional body executor; {@code null} keeps the engine default.
     * @return the observed outcome.
     */
        public static ReusedNamesOutcome reusedNamesLiveLock(long seed, String orderId,
                                                         java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.loopingPollReusedNamesWorkflow(effects),
                                             bodyExecutorOverride)) {
            String workflowId = "loopr-" + orderId;

            world.engine().publish(new LoopReusedPollRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "iteration 1's poll to park",
                                () -> hasStepRecord(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_POLL,
                                                    StepStatus.STARTED));

            // Fire iteration 1's poll timeout (era-anchored). The sleep that follows usually resolves on the same
            // advance (its wall-stamped deadline lags the advanced virtual clock); nudges flush it if it parked.
            advancePastPollTimeout(world, workflowId, LoopingPollWorkflow.STEP_POLL);
            for (int i = 0; i < 10 && terminalWorkflowStatus(world.committedLog(), workflowId) == null; i++) {
                world.advanceTime(NUDGE);
                Polling.await(Duration.ofMillis(500),
                              () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);
            }
            Polling.awaitOrFail(DEADLINE, "the bounded loop to exhaust and fail",
                                () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);

            // NOT asserting INV-2 here: this probe SURFACED an INV-2 violation (the duplicate retryDelay TIMED_OUT)
            // — the gap is returned as an observable and pinned by the test as an expected violation.
            return new ReusedNamesOutcome(
                    effects.count(workflowId, LoopingPollWorkflow.EFFECT_ITERATION),
                    stepStatusRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_POLL,
                                      StepStatus.STARTED),
                    stepTerminalRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_POLL),
                    stepRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_RETRY_DELAY),
                    stepTerminalRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_RETRY_DELAY),
                    terminalWorkflowStatus(world.committedLog(), workflowId));
        }
    }

    /**
     * The counter-names contrast: the documented correct authoring; the signal arrives during iteration 1's park and
     * the loop exits cleanly through its success path. (Multi-timeout loop pacing is not deterministically drivable
     * under the D5 era residual — each post-advance iteration's wall-stamped deadline races the advanced virtual
     * clock — so the contrast pins the success path; see the campaign notes.)
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code loopc-<orderId>}).
     * @return the observed outcome.
     */
        public static CounterNamesOutcome counterNamesCompletesOnSignal(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.loopingPollCounterNamesWorkflow(effects))) {
            String workflowId = "loopc-" + orderId;

            world.engine().publish(new LoopCounterPollRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "iteration 1's poll to park",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    LoopingPollWorkflow.STEP_POLL + "-1", StepStatus.STARTED));

            world.engine().publish(new PollSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the loop to complete on the delivered signal",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new CounterNamesOutcome(
                    effects.count(workflowId, LoopingPollWorkflow.EFFECT_ITERATION),
                    terminalWorkflowStatus(world.committedLog(), workflowId),
                    effects.count(workflowId, LoopingPollWorkflow.STEP_PROCESS));
        }
    }

    /**
     * The aligned-clock multi-iteration pacing pin: the counter-names loop driven through MULTIPLE genuine iterations
     * under the OPT-IN aligned-clock mode ({@code SimulationWorld(seed, registration, true)} — the static Axon
     * event-timestamp clock and the virtual clock share one era, so the engine's recorded-STARTED-vs-now timer math is
     * exact). Iteration 1's poll times out on a plain {@code advanceTime(10s)} — no era anchoring — its 2s sleep
     * genuinely paces (resolving only on a 2s advance), iteration 2 times out the same way, and iteration 3 receives
     * the signal and completes. This is exactly the multi-timeout sequence the D5 era residual made undrivable: after
     * the first era-anchored advance every later wall-stamped deadline raced the advanced virtual clock; aligned mode
     * removes the race, so each iteration's wait/sleep is driven by its own exact virtual advance.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code loopc-<orderId>}).
     * @return the observed outcome.
     */
        public static AlignedPacingOutcome alignedCounterNamesMultiIterationPacing(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.loopingPollCounterNamesWorkflow(effects), true)) {
            String workflowId = "loopc-" + orderId;

            world.engine().publish(new LoopCounterPollRequestedEvent(orderId));

            // Iterations 1..COUNTER_TIMEOUT_ITERATIONS: each poll genuinely parks, times out on an exact 10s virtual
            // advance, and its sleep resolves only on the following 2s advance — per-iteration pacing, no anchoring.
            for (int i = 1; i <= LoopingPollWorkflow.COUNTER_TIMEOUT_ITERATIONS; i++) {
                String poll = LoopingPollWorkflow.STEP_POLL + "-" + i;
                String sleep = LoopingPollWorkflow.STEP_RETRY_DELAY + "-" + i;
                Polling.awaitOrFail(DEADLINE, "iteration " + i + "'s poll to park",
                                    () -> hasStepRecord(world.committedLog(), workflowId, poll, StepStatus.STARTED));
                Polling.awaitOrFail(DEADLINE, "iteration " + i + "'s poll timeout to be scheduled",
                                    () -> world.scheduler().pendingTasks() > 0);
                // Aligned eras make the timer math exact: STARTED is stamped at virtual now, so the timeout fires on
                // a plain POLL_TIMEOUT advance (+1s slack for the strictly-after comparison).
                world.advanceTime(LoopingPollWorkflow.POLL_TIMEOUT.plusSeconds(1));
                Polling.awaitOrFail(DEADLINE, "iteration " + i + "'s poll to time out",
                                    () -> hasStepRecord(world.committedLog(), workflowId, poll, StepStatus.TIMED_OUT));
                // The (non-blocking, F-19) sleep registers immediately after the timeout; it must resolve only once
                // its own 2s of virtual time elapse — the pacing the era residual previously broke.
                Polling.awaitOrFail(DEADLINE, "iteration " + i + "'s retry sleep to register",
                                    () -> stepRecords(world.committedLog(), workflowId, sleep) >= 1);
                world.advanceTime(LoopingPollWorkflow.RETRY_DELAY.plusSeconds(1));
                Polling.awaitOrFail(DEADLINE, "iteration " + i + "'s retry sleep to resolve",
                                    () -> stepTerminalRecords(world.committedLog(), workflowId, sleep) >= 1);
            }

            // Iteration 3: the poll parks again (re-registered — counter names) and the signal completes it, with NO
            // further time advance, so its own pending timeout never fires.
            String poll3 = LoopingPollWorkflow.STEP_POLL + "-" + (LoopingPollWorkflow.COUNTER_TIMEOUT_ITERATIONS + 1);
            Polling.awaitOrFail(DEADLINE, "iteration 3's poll to park",
                                () -> hasStepRecord(world.committedLog(), workflowId, poll3, StepStatus.STARTED));
            world.engine().publish(new PollSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the loop to complete on the delivered signal",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new AlignedPacingOutcome(
                    effects.count(workflowId, LoopingPollWorkflow.EFFECT_ITERATION),
                    stepStatusRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_POLL + "-1",
                                      StepStatus.TIMED_OUT),
                    stepStatusRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_POLL + "-2",
                                      StepStatus.TIMED_OUT),
                    stepStatusRecords(world.committedLog(), workflowId, poll3, StepStatus.COMPLETED),
                    stepTerminalRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_RETRY_DELAY + "-1"),
                    stepTerminalRecords(world.committedLog(), workflowId, LoopingPollWorkflow.STEP_RETRY_DELAY + "-2"),
                    effects.count(workflowId, LoopingPollWorkflow.STEP_PROCESS),
                    terminalWorkflowStatus(world.committedLog(), workflowId));
        }
    }

    /**
     * The broadcast storm: {@code waiters} instances of the correlated-wait workflow all park on the SAME key; one
     * broadcast signal must wake every one of them, each exactly once.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param waiters how many instances park on the shared key.
     * @return the observed outcome.
     */
        public static BroadcastOutcome broadcastWakesAllWaiters(long seed, int waiters) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.correlatedWaitWorkflow(effects))) {
            String sharedKey = "G";

            for (int i = 1; i <= waiters; i++) {
                world.engine().publish(new CorrelatedWaitRequestedEvent("b" + i, sharedKey));
            }
            for (int i = 1; i <= waiters; i++) {
                String workflowId = "corr-b" + i;
                Polling.awaitOrFail(DEADLINE, "waiter " + workflowId + " to park on the shared key",
                                    () -> hasStepRecord(world.committedLog(), workflowId,
                                                        CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL,
                                                        StepStatus.STARTED));
            }

            // ONE broadcast signal for the shared key.
            world.engine().publish(new CorrelatedSignalEvent(sharedKey));
            for (int i = 1; i <= waiters; i++) {
                String workflowId = "corr-b" + i;
                Polling.awaitOrFail(DEADLINE, "waiter " + workflowId + " to complete after the broadcast",
                                    () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                                WorkflowStatus.COMPLETED) >= 1);
            }

            Invariants.assertAtMostOnceRecording(world.committedLog());
            int completed = 0;
            int matches = 0;
            for (int i = 1; i <= waiters; i++) {
                String workflowId = "corr-b" + i;
                if (terminalWorkflowStatus(world.committedLog(), workflowId) == WorkflowStatus.COMPLETED) {
                    completed++;
                }
                matches += effects.count(workflowId, CorrelatedWaitWorkflow.STEP_RECORD_MATCH);
            }
            return new BroadcastOutcome(completed, waiters, matches);
        }
    }

    /**
     * Fires the named poll wait's timeout deterministically (era-anchored on its recorded STARTED timestamp — the
     * Inv9 precedent).
     */
    private static void advancePastPollTimeout(SimulationWorld world, String workflowId,
                                               String stepName) {
        Polling.awaitOrFail(DEADLINE, "the poll-timeout to be scheduled on the virtual scheduler",
                            () -> world.scheduler().pendingTasks() > 0);
        Instant started = startedAt(world.committedLog(), workflowId, stepName)
                .orElseThrow(() -> new IllegalStateException(stepName + " has no STARTED record"));
        Instant fireBy = started.plus(LoopingPollWorkflow.POLL_TIMEOUT).plusSeconds(1);
        Duration advance = Duration.between(world.clock().instant(), fireBy);
        world.advanceTime(advance.isNegative() ? Duration.ZERO : advance);
    }

    /**
     * Whether the instance's committed log holds a record for {@code (stepName, status)}.
     */
    private static boolean hasStepRecord(List<EventMessage> committedLog, String workflowId,
                                         String stepName, StepStatus status) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .anyMatch(e -> stepName.equals(MetadataUtils.getStepName(e.metadata()))
                                   && MetadataUtils.getStepStatus(e.metadata()).map(status::equals).orElse(false));
    }

    /**
     * How many records of {@code (stepName, status)} the instance's committed log holds.
     */
    private static int stepStatusRecords(List<EventMessage> committedLog, String workflowId,
                                         String stepName, StepStatus status) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> stepName.equals(MetadataUtils.getStepName(e.metadata())))
                                 .filter(e -> MetadataUtils.getStepStatus(e.metadata())
                                                           .map(status::equals).orElse(false))
                                 .count();
    }

    /**
     * How many records (any step status) the instance's committed log holds for {@code stepName}.
     */
    private static int stepRecords(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> stepName.equals(MetadataUtils.getStepName(e.metadata())))
                                 .filter(e -> MetadataUtils.getStepStatus(e.metadata()).isPresent())
                                 .count();
    }

    /**
     * How many TERMINAL records the instance's committed log holds for {@code stepName}.
     */
    private static int stepTerminalRecords(List<EventMessage> committedLog, String workflowId,
                                           String stepName) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> stepName.equals(MetadataUtils.getStepName(e.metadata())))
                                 .filter(e -> MetadataUtils.getStepStatus(e.metadata())
                                                           .map(StepStatus::isTerminal).orElse(false))
                                 .count();
    }

    /**
     * How many committed workflow-status records of {@code status} the instance has.
     */
    private static int workflowStatusRecords(List<EventMessage> committedLog, String workflowId,
                                             WorkflowStatus status) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                                           .map(status::equals).orElse(false))
                                 .count();
    }

    /**
     * The instance's first committed terminal workflow status, or {@code null}.
     */
    @Nullable
    private static WorkflowStatus terminalWorkflowStatus(List<EventMessage> committedLog,
                                                         String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .map(e -> MetadataUtils.getWorkflowStatus(e.metadata()).orElse(null))
                           .filter(s -> s != null && s.isTerminal())
                           .findFirst()
                           .orElse(null);
    }

    /**
     * The event-store timestamp of the step's STARTED record within the instance, if any.
     */
        private static Optional<Instant> startedAt(List<EventMessage> committedLog, String workflowId,
                                               String stepName) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                                   && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                                   && MetadataUtils.getStepStatus(e.metadata())
                                                   .map(s -> s == StepStatus.STARTED).orElse(false))
                           .map(EventMessage::timestamp)
                           .findFirst();
    }
}
