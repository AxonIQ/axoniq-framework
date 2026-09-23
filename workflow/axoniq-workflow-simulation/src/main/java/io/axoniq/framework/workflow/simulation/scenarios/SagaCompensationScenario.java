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
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FulfillmentConfirmedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaDefaultTimeoutOrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaOrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaRetryCompOrderPlacedEvent;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * Phase-1 production-realism scenarios: a saga-style order-fulfillment workflow with <strong>compensation steps inside
 * catch blocks</strong> (the {@link SagaOrderWorkflow}), driven through the crash windows a real production deployment
 * meets. The forward path, both compensation branches, the crash-mid-compensation window (contrasting the no-retry and
 * retrying compensation authoring), the confirmation-vs-timeout race, and the F-13-class FAIL-path duplicate terminal
 * record are each a deterministic, named run.
 * <p>
 * All observables are content-based and per-{@code workflowId} (F-2-robust — never the global append order).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class SagaCompensationScenario {

    /**
     * Bounded wall-clock deadline for poll steps; the virtual clock is advanced explicitly where timers must fire.
     */
    private static final Duration DEADLINE = Duration.ofSeconds(10);

    /**
     * Bounded observation window for conditions expected to stay ABSENT (e.g. "no terminal status appears" on the
     * wedge path) — short so the scenario never crawls, long enough that queued work would have surfaced.
     */
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(3);

    /**
     * Per-nudge virtual-time advance used to flush retry backoff timers (the saga's backoffs are 100ms).
     */
    private static final Duration BACKOFF_NUDGE = Duration.ofMillis(500);

    /**
     * Maximum backoff nudges before a poll is declared failed (bounded — the scenario can never hang).
     */
    private static final int MAX_NUDGES = 20;

    private SagaCompensationScenario() {
    }

    /**
     * Terminal snapshot of one saga instance: the workflow's terminal status plus every effect counter.
     *
     * @param terminalStatus the workflow's terminal status, or {@code null} if the instance never reached one.
     * @param reserveCount   {@code reserveStock} effect executions.
     * @param chargeCount    {@code chargePayment} effect executions (attempts).
     * @param notifyCount    {@code notifyCustomer} effect executions.
     * @param releaseCount   {@code releaseStock} compensation effect executions.
     * @param refundCount    {@code refundPayment} compensation effect executions.
     */
    public record SagaOutcome(@Nullable WorkflowStatus terminalStatus, int reserveCount, int chargeCount,
                              int notifyCount, int releaseCount, int refundCount) {

    }

    /**
     * Outcome of the crash-mid-compensation window.
     *
     * @param terminalStatus        the workflow's terminal status after recovery, or {@code null} if the instance
     *                              wedged non-terminally (the no-retry expectation).
     * @param releaseTerminalStatus the terminal step status {@code releaseStock} resolved to after recovery
     *                              ({@code FAILED} under no-retry — the indeterminate resolution; {@code COMPLETED}
     *                              under retry), or {@code null} if none.
     * @param releaseEffectCount    {@code releaseStock} effect executions (1 under no-retry — not re-run; 2 under
     *                              retry — a fresh attempt).
     * @param refundEffectCount     {@code refundPayment} effect executions (0 under no-retry — never reached; 1 under
     *                              retry).
     * @param cancelledRecords      committed {@code <workflow>:CANCELLED} records (0 under no-retry; 1 under retry).
     * @param workflowTerminalRecords committed workflow terminal records after one further restart (1).
     * @param liveAtEnd             whether the instance is still live after that further restart ({@code false}).
     */
    public record CrashMidCompensationOutcome(@Nullable WorkflowStatus terminalStatus,
                                              @Nullable StepStatus releaseTerminalStatus,
                                              int releaseEffectCount, int refundEffectCount, int cancelledRecords,
                                              int workflowTerminalRecords, boolean liveAtEnd) {

    }

    /**
     * Outcome of a confirmation-vs-timeout race run.
     *
     * @param waitTerminalRecords    committed terminal records for the {@code awaitFulfillment} step — the core
     *                               consistency observable (must be exactly 1).
     * @param waitTerminalStatus     the wait step's terminal status (the race winner), or {@code null} if none.
     * @param workflowTerminalStatus the workflow's terminal status, or {@code null} if none.
     * @param workflowTerminalRecords committed workflow-status terminal records for the instance (must be exactly 1).
     * @param notifyCount            {@code notifyCustomer} effect executions (1 iff the confirmation won).
     * @param releaseCount           {@code releaseStock} effect executions (1 iff the timeout won).
     * @param refundCount            {@code refundPayment} effect executions (1 iff the timeout won).
     */
    public record RaceOutcome(int waitTerminalRecords, @Nullable StepStatus waitTerminalStatus,
                              @Nullable WorkflowStatus workflowTerminalStatus, int workflowTerminalRecords,
                              int notifyCount, int releaseCount, int refundCount) {

    }

    /**
     * Outcome of the F-13-class FAIL-path duplicate-terminal-record run.
     *
     * @param failedRecordsBeforeReDrive committed {@code <workflow>:FAILED} records after the crash + recover but
     *                                   before the re-drive — exactly 1 (a crash/replay alone appends nothing).
     * @param failedRecordsAfterReDrive  committed {@code <workflow>:FAILED} records after the deterministic F-3
     *                                   start-event redelivery re-drives the body to {@code ctx.fail(...)} — 2 under
     *                                   the F-13-class gap.
     * @param releaseEffectCount         {@code releaseStock} compensation effect executions across the whole run — 2,
     *                                   because the F-3 re-spawn builds FRESH state from the start event (it never
     *                                   consults the durable log), so the re-driven body re-runs every action
     *                                   including the compensation side effects (in production terms: the stock is
     *                                   released twice and a second FAILED terminal is recorded for an order that
     *                                   already failed).
     */
    public record DuplicateFailedOutcome(int failedRecordsBeforeReDrive, int failedRecordsAfterReDrive,
                                         int releaseEffectCount) {

    }

    /**
     * Happy path: charge succeeds, the fulfillment confirmation arrives in time, the saga completes; no compensation
     * effect runs.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code saga-<orderId>}).
     * @return the terminal snapshot.
     */
        public static SagaOutcome happyPath(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.sagaOrderWorkflow(effects))) {
            String workflowId = "saga-" + orderId;

            world.engine().publish(new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_HAPPY));
            Polling.awaitOrFail(DEADLINE, "awaitFulfillment to be STARTED",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT, StepStatus.STARTED));

            world.engine().publish(new FulfillmentConfirmedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "saga to COMPLETE",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return snapshot(world, effects, workflowId);
        }
    }

    /**
     * Charge-declined path: the charge action throws on every attempt, exhausts its retry policy, and the catch runs
     * the failure-compensation branch ({@code releaseStock} only) before {@code ctx.fail(...)}.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the terminal snapshot.
     */
        public static SagaOutcome chargeDeclined(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.sagaOrderWorkflow(effects))) {
            String workflowId = "saga-" + orderId;

            world.engine().publish(new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_CHARGE_DECLINED));
            advanceUntilOrFail(world, "saga to reach terminal FAILED after charge exhaustion + compensation",
                               () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                           WorkflowStatus.FAILED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return snapshot(world, effects, workflowId);
        }
    }

    /**
     * Fulfillment-timeout path: charge succeeds, the confirmation never arrives, the wait times out and the catch runs
     * the full compensation chain ({@code releaseStock} + {@code refundPayment}) before {@code ctx.cancel()}.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the terminal snapshot.
     */
        public static SagaOutcome fulfillmentTimeout(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.sagaOrderWorkflow(effects))) {
            String workflowId = "saga-" + orderId;

            world.engine().publish(new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_HAPPY));
            Polling.awaitOrFail(DEADLINE, "awaitFulfillment to be STARTED",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT, StepStatus.STARTED));

            advancePastFulfillmentTimeout(world, workflowId);
            Polling.awaitOrFail(DEADLINE, "saga to reach terminal CANCELLED after timeout compensation",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.CANCELLED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return snapshot(world, effects, workflowId);
        }
    }

    /**
     * The crash-mid-compensation window: the wait times out, the catch starts compensating, the {@code releaseStock}
     * COMPLETED commit vanishes (the write-then-vanish window), and the engine crashes + recovers. The recovered body
     * re-takes the timeout branch and re-reaches {@code releaseStock}, which is {@code STARTED}-at-entry — the
     * engine's at-most-once resolution decides the saga's fate:
     * <ul>
     *   <li><strong>no-retry compensation</strong> ({@code retryingCompensation=false}): the step resolves to FAILED
     *       ({@code StepIndeterminateException}), the {@code StepFailedException} escapes the catch block uncaught,
     *       and the {@code handleWorkflowException} default branch parks the instance non-terminally — the saga
     *       wedges half-compensated (no refund, no CANCELLED);</li>
     *   <li><strong>retrying compensation</strong> ({@code retryingCompensation=true}): the step resolves to RETRYING
     *       + a fresh attempt, compensation completes, and the saga terminates CANCELLED.</li>
     * </ul>
     *
     * @param seed                  seed for the world's deterministic id source.
     * @param orderId               business key for the single instance.
     * @param retryingCompensation  which saga variant to run (and which start event to publish).
     * @return the observed outcome.
     */
        public static CrashMidCompensationOutcome crashMidCompensation(long seed, String orderId,
                                                                   boolean retryingCompensation) {
        var effects = new CountingEffects();
        var registration = retryingCompensation
                ? EngineInstance.sagaRetryCompOrderWorkflow(effects)
                : EngineInstance.sagaOrderWorkflow(effects);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = (retryingCompensation ? "sagarc-" : "saga-") + orderId;

            Object start = retryingCompensation
                    ? new SagaRetryCompOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_HAPPY)
                    : new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_HAPPY);
            world.engine().publish(start);
            Polling.awaitOrFail(DEADLINE, "awaitFulfillment to be STARTED",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT, StepStatus.STARTED));

            // Arm the write-then-vanish window on the FIRST compensation step's terminal commit, then fire the wait
            // timeout: the catch runs, releaseStock's action executes (effect #1), and its COMPLETED vanishes — the
            // step is left STARTED in the durable log with the instance parked mid-compensation.
            world.eventStore().armVanishCommitFor(SagaOrderWorkflow.STEP_RELEASE_STOCK, StepStatus.COMPLETED);
            advancePastFulfillmentTimeout(world, workflowId);
            Polling.awaitOrFail(DEADLINE, "releaseStock compensation effect to run once",
                                () -> effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK) >= 1);
            Polling.awaitOrFail(DEADLINE, "releaseStock COMPLETED commit to vanish",
                                () -> !world.eventStore().isVanishArmed());

            var beforeCrash = List.copyOf(world.committedLog());
            world.crashAndRecover();
            Invariants.assertCommittedHistorySurvivesCrash(beforeCrash, world.committedLog());

            if (retryingCompensation) {
                // The interrupted attempt resolves to RETRYING + a fresh attempt; compensation completes and the saga
                // cancels. Backoff timers need virtual-time nudges.
                advanceUntilOrFail(world, "retrying saga to terminate CANCELLED after recovered compensation",
                                   () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                               WorkflowStatus.CANCELLED) >= 1);
            } else {
                // The interrupted attempt resolves to FAILED (indeterminate); the failure escapes the catch block
                // uncaught, which ends the workflow FAILED.
                Polling.awaitOrFail(DEADLINE, "releaseStock to resolve to a terminal record after recovery",
                                    () -> stepTerminalStatus(world.committedLog(), workflowId,
                                                             SagaOrderWorkflow.STEP_RELEASE_STOCK) != null);
                Polling.await(DEADLINE, () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);
            }

            // One more restart after the terminal: nothing may re-drive the instance or publish a second terminal.
            int releasesBeforeRestart = effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK);
            world.crashAndRecover();
            Polling.await(ABSENCE_WINDOW,
                          () -> workflowTerminalRecords(world.committedLog(), workflowId) > 1
                                  || effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK)
                                  > releasesBeforeRestart);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new CrashMidCompensationOutcome(
                    terminalWorkflowStatus(world.committedLog(), workflowId),
                    stepTerminalStatus(world.committedLog(), workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK),
                    effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK),
                    effects.count(workflowId, SagaOrderWorkflow.STEP_REFUND_PAYMENT),
                    workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.CANCELLED),
                    workflowTerminalRecords(world.committedLog(), workflowId),
                    world.engine().liveWorkflowIds().contains(workflowId));
        }
    }

    /**
     * The confirmation-vs-timeout race: both stimuli (the matching {@link FulfillmentConfirmedEvent} and a virtual-time
     * advance past the wait timeout) are applied back-to-back with no settle between them, in the given order. The
     * consistency contract under ANY winner: exactly one terminal record for the wait step, exactly one workflow
     * terminal record, and a downstream branch consistent with the winner (never a mix of {@code notifyCustomer} and
     * compensation).
     *
     * @param seed       seed for the world's deterministic id source.
     * @param orderId    business key for the single instance.
     * @param eventFirst {@code true} to publish the confirmation then advance time; {@code false} to advance time then
     *                   publish the (now late) confirmation.
     * @return the observed outcome.
     */
        public static RaceOutcome confirmationVsTimeoutRace(long seed, String orderId, boolean eventFirst) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.sagaOrderWorkflow(effects))) {
            String workflowId = "saga-" + orderId;

            world.engine().publish(new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_HAPPY));
            Polling.awaitOrFail(DEADLINE, "awaitFulfillment to be STARTED",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT, StepStatus.STARTED));

            // Both stimuli back-to-back, NO settle between them. The advance is anchored on the wait's recorded
            // STARTED timestamp (see advancePastFulfillmentTimeout); the pre-computation happens before either
            // stimulus so the two land in the same window.
            Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled on the virtual scheduler",
                                () -> world.scheduler().pendingTasks() > 0);
            Duration advance = fulfillmentTimeoutAdvance(world, workflowId);
            if (eventFirst) {
                world.engine().publish(new FulfillmentConfirmedEvent(orderId));
                world.advanceTime(advance);
            } else {
                world.advanceTime(advance);
                world.engine().publish(new FulfillmentConfirmedEvent(orderId));
            }

            Polling.awaitOrFail(DEADLINE, "saga to reach a terminal workflow status after the race",
                                () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);
            // Give any racing second terminal a short window to surface before counting.
            Polling.await(ABSENCE_WINDOW,
                          () -> stepTerminalRecords(world.committedLog(), workflowId,
                                                    SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT) > 1
                                  || workflowTerminalRecords(world.committedLog(), workflowId) > 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new RaceOutcome(
                    stepTerminalRecords(world.committedLog(), workflowId, SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT),
                    stepTerminalStatus(world.committedLog(), workflowId, SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT),
                    terminalWorkflowStatus(world.committedLog(), workflowId),
                    workflowTerminalRecords(world.committedLog(), workflowId),
                    effects.count(workflowId, SagaOrderWorkflow.STEP_NOTIFY_CUSTOMER),
                    effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK),
                    effects.count(workflowId, SagaOrderWorkflow.STEP_REFUND_PAYMENT));
        }
    }

    /**
     * The F-13-class FAIL-path duplicate: drives the saga to terminal {@code <workflow>:FAILED} via the
     * charge-declined compensation branch, crashes + recovers, then re-delivers the start event (the deterministic F-3
     * restart trigger). The re-spawned execution starts with FRESH state, so the body re-runs from scratch and
     * re-reaches {@code ctx.fail(...)} — observing whether {@code TerminateDelegate.failed}'s ungated direct publish
     * commits a SECOND {@code <workflow>:FAILED} for the same {@code workflowId} (the FAIL-path analogue of the
     * documented F-13 cancel-path duplicate, via F-13's own deterministic trigger).
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static DuplicateFailedOutcome duplicateFailedTerminalRecord(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.sagaOrderWorkflow(effects))) {
            String workflowId = "saga-" + orderId;

            world.engine().publish(new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_CHARGE_DECLINED));
            advanceUntilOrFail(world, "saga to reach terminal FAILED",
                               () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                           WorkflowStatus.FAILED) >= 1);

            world.crashAndRecover();
            int beforeReDrive = workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.FAILED);

            // Deterministic F-3 restart trigger: re-deliver the start event for the already-terminated business key.
            // The re-spawned execution starts with FRESH state (checkAndCreateNewWorkflow builds it from the start
            // event's payload — it never consults the durable log), so the body re-runs from scratch: new attempts,
            // re-run effects, and a SECOND ctx.fail(...). The fresh charge retries need backoff nudges.
            world.engine().publish(new SagaOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_CHARGE_DECLINED));
            advanceUntil(world, () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.FAILED) >= 2);

            return new DuplicateFailedOutcome(
                    beforeReDrive,
                    workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.FAILED),
                    effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK));
        }
    }

    /**
     * Outcome of the doomed-attempt probe.
     *
     * @param releaseEffectCount      {@code releaseStock} effect executions — 1 under the gap: the action RAN even
     *                                though its per-attempt deadline had already elapsed at entry.
     * @param releaseTerminalStatus   the terminal status {@code releaseStock} recorded — {@code TIMED_OUT} under the
     *                                gap (the executed action's result is discarded).
     * @param releaseCompletedRecords committed COMPLETED records for {@code releaseStock} — 0 under the gap.
     * @param workflowTerminalStatus  the workflow's terminal status, or {@code null} — under the gap the
     *                                {@code StepTimedOutException} escapes the catch block uncaught, which ends the
     *                                workflow {@code FAILED}.
     * @param workflowTerminalRecords committed workflow terminal records after one further restart (1).
     * @param liveAtEnd               whether the instance is still live after that further restart ({@code false}).
     */
    public record DoomedAttemptOutcome(int releaseEffectCount, @Nullable StepStatus releaseTerminalStatus,
                                       int releaseCompletedRecords, @Nullable WorkflowStatus workflowTerminalStatus,
                                       int workflowTerminalRecords, boolean liveAtEnd) {

    }

    /**
     * The doomed-attempt probe: the default-timeout saga rides the fulfillment-timeout branch, so the compensation
     * step {@code releaseStock} is entered AFTER the clock moved past what will become its own
     * {@code STARTED + 5s} deadline (the wait-timeout advance left the injected clock ahead of the statically-stamped
     * event timestamps — the production analogue is a forward clock jump between a step's STARTED commit and its
     * deadline computation). {@code ExecuteDelegate} computes {@code remainingTimeout} at entry
     * ({@code ExecuteDelegate.java:149-151}) but dispatches the action FIRST ({@code :154-172}) and only then checks
     * {@code remainingTimeout.isNegative()} ({@code :176}) — on the negative path the action's result future gets NO
     * completion handler: the side effect runs, its result is discarded, and the step records TIMED_OUT. The
     * {@code StepTimedOutException} then escapes the saga's catch block uncaught and the instance wedges
     * non-terminally (the {@code handleWorkflowException} default-branch sink).
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static DoomedAttemptOutcome doomedCompensationAfterClockJump(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.sagaDefaultTimeoutOrderWorkflow(effects))) {
            String workflowId = "sagadt-" + orderId;

            world.engine().publish(new SagaDefaultTimeoutOrderPlacedEvent(orderId, SagaOrderWorkflow.MODE_HAPPY));
            Polling.awaitOrFail(DEADLINE, "awaitFulfillment to be STARTED",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT, StepStatus.STARTED));

            // Fire the wait timeout — the advance leaves the injected clock past the wall-stamped event timestamps,
            // so every subsequent default-timeout step is past-deadline at entry (the production clock-jump analogue).
            advancePastFulfillmentTimeout(world, workflowId);

            // The doomed attempt: the compensation action RUNS (effect lands)...
            Polling.awaitOrFail(DEADLINE, "the doomed releaseStock action to run",
                                () -> effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK) >= 1);
            // ...and the step resolves to a terminal record (TIMED_OUT — the result is discarded).
            Polling.awaitOrFail(DEADLINE, "releaseStock to resolve to a terminal record",
                                () -> stepTerminalStatus(world.committedLog(), workflowId,
                                                         SagaOrderWorkflow.STEP_RELEASE_STOCK) != null);
            // The StepTimedOutException escapes the catch block uncaught, which ends the workflow FAILED.
            Polling.await(DEADLINE, () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);

            // One more restart after the terminal: nothing may re-drive the instance or publish a second terminal.
            world.crashAndRecover();
            Polling.await(ABSENCE_WINDOW,
                          () -> workflowTerminalRecords(world.committedLog(), workflowId) > 1
                                  || effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK) > 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new DoomedAttemptOutcome(
                    effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK),
                    stepTerminalStatus(world.committedLog(), workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK),
                    stepStatusRecords(world.committedLog(), workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK,
                                      StepStatus.COMPLETED),
                    terminalWorkflowStatus(world.committedLog(), workflowId),
                    workflowTerminalRecords(world.committedLog(), workflowId),
                    world.engine().liveWorkflowIds().contains(workflowId));
        }
    }

    /**
     * Builds the terminal snapshot for one instance.
     */
        private static SagaOutcome snapshot(SimulationWorld world, CountingEffects effects,
                                        String workflowId) {
        return new SagaOutcome(
                terminalWorkflowStatus(world.committedLog(), workflowId),
                effects.count(workflowId, SagaOrderWorkflow.STEP_RESERVE_STOCK),
                effects.count(workflowId, SagaOrderWorkflow.STEP_CHARGE_PAYMENT),
                effects.count(workflowId, SagaOrderWorkflow.STEP_NOTIFY_CUSTOMER),
                effects.count(workflowId, SagaOrderWorkflow.STEP_RELEASE_STOCK),
                effects.count(workflowId, SagaOrderWorkflow.STEP_REFUND_PAYMENT));
    }

    /**
     * Polls the condition under bounded virtual-time nudges (to flush retry-backoff timers), failing loudly when the
     * nudge budget is exhausted — the scenario can never hang.
     */
    private static void advanceUntilOrFail(SimulationWorld world, String description,
                                           BooleanSupplier condition) {
        advanceUntil(world, condition);
        Polling.awaitOrFail(Duration.ofSeconds(5), description, condition);
    }

    /**
     * The non-failing nudge loop behind {@link #advanceUntilOrFail} — also used where the caller asserts the outcome
     * itself (e.g. the duplicate-FAILED count, whose absence is a meaningful result rather than a harness failure).
     */
    private static void advanceUntil(SimulationWorld world, BooleanSupplier condition) {
        for (int i = 0; i < MAX_NUDGES && !condition.getAsBoolean(); i++) {
            world.advanceTime(BACKOFF_NUDGE);
            Polling.await(Duration.ofMillis(300), condition);
        }
    }

    /**
     * Fires the {@code awaitFulfillment} wait timeout deterministically: waits for the timeout continuation to be
     * registered on the virtual scheduler, then advances virtual time past the deadline anchored on the wait step's
     * <em>recorded STARTED timestamp</em>. The anchoring matters: the in-memory store stamps events from the static
     * Axon {@code GenericEventMessage} clock (the un-injectable residual — ARCHITECTURE §12 / adoc D5) while the
     * harness {@code MutableClock} starts at the Unix epoch, so a fixed-delta advance would never cross the deadline
     * (the Inv9TimeoutsFireScenario precedent).
     */
    private static void advancePastFulfillmentTimeout(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled on the virtual scheduler",
                            () -> world.scheduler().pendingTasks() > 0);
        world.advanceTime(fulfillmentTimeoutAdvance(world, workflowId));
    }

    /**
     * The virtual-time delta that crosses the {@code awaitFulfillment} timeout deadline, anchored on the recorded
     * STARTED timestamp (see {@link #advancePastFulfillmentTimeout}).
     */
        private static Duration fulfillmentTimeoutAdvance(SimulationWorld world, String workflowId) {
        Instant started = startedAt(world.committedLog(), workflowId, SagaOrderWorkflow.STEP_AWAIT_FULFILLMENT)
                .orElseThrow(() -> new IllegalStateException("awaitFulfillment has no STARTED record"));
        Instant fireBy = started.plus(SagaOrderWorkflow.FULFILLMENT_TIMEOUT).plusSeconds(1);
        Duration advance = Duration.between(world.clock().instant(), fireBy);
        return advance.isNegative() ? Duration.ZERO : advance;
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
     * The first terminal step status recorded for {@code stepName} within the instance, or {@code null} if none.
     */
    @Nullable
    private static StepStatus stepTerminalStatus(List<EventMessage> committedLog, String workflowId,
                                                 String stepName) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .filter(e -> stepName.equals(MetadataUtils.getStepName(e.metadata())))
                           .map(e -> MetadataUtils.getStepStatus(e.metadata()).orElse(null))
                           .filter(s -> s != null && s.isTerminal())
                           .findFirst()
                           .orElse(null);
    }

    /**
     * How many terminal records {@code stepName} has within the instance's committed log.
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
     * How many committed TERMINAL workflow-status records the instance has (any terminal status).
     */
    private static int workflowTerminalRecords(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                                           .map(WorkflowStatus::isTerminal).orElse(false))
                                 .count();
    }

    /**
     * The instance's first committed terminal workflow status, or {@code null} if it has none.
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
}
