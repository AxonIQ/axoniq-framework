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

import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OnRetryFires;
import io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryEdgesRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryTimeoutRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * The retry-attempt gate (issue #408) combined with the two faults that hit a running retry attempt: a crash while the
 * attempt is in flight, and an external cancellation while the attempt is in flight.
 * <p>
 * Every retry attempt records {@code RETRY_STARTED} and runs only once the store accepted it, so the durable log can
 * now tell "backoff pending" ({@code RETRYING}) from "attempt in flight" ({@code RETRY_STARTED}). Two consequences are
 * pinned here:
 * <ul>
 *   <li>{@link #runCrashDuringRetryAttempt(long, String)} — the engine crashes while attempt 2 is in flight (its
 *       failing outcome commit is dropped, so the durable log ends with {@code RETRY_STARTED(2)}). Recovery must treat
 *       the attempt as indeterminate with its recorded number: {@code RETRYING(2)}, then attempt 3 as
 *       {@code RETRY_STARTED(3)}, then {@code COMPLETED}. Attempt 2 is never re-run in place, the attempt count never
 *       restarts at 1, and INV-8 keeps its bound.</li>
 *   <li>{@link #runExternalCancelDuringRetryAttempt(long, String)} — the step is cancelled externally while attempt 2
 *       is running. The step records {@code CANCELLED}, no further {@code RETRYING} / {@code RETRY_STARTED} appears, and
 *       the effect counter does not move after the cancel.</li>
 * </ul>
 * Scenario-pinned only (registrations are scenario-only, not in {@link SimulationWorld} defaults).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class RetryStartedGateScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);

    private RetryStartedGateScenario() {
    }

    /**
     * Observables of the crash-during-retry-attempt drive.
     *
     * @param effectsBeforeCrash         action runs before the crash (expected 2: attempts 1 and 2 both failed).
     * @param lastRecordBeforeCrash      the step's last durable record before the crash (expected
     *                                   {@code RETRY_STARTED}: the fault landed, attempt 2 was in flight).
     * @param effectsAfterRecovery       action runs once the step completed (expected 3: attempt 3 ran once, attempt 2
     *                                   was not re-run).
     * @param retryingAttempts           attempt numbers on the committed {@code RETRYING} records (expected [1, 2]).
     * @param retryStartedAttempts       attempt numbers on the committed {@code RETRY_STARTED} records (expected
     *                                   [2, 3]).
     * @param completedRecords           committed {@code COMPLETED} records for the step (expected 1).
     * @param workflowReachedTerminal    whether the workflow recorded a terminal status.
     */
    public record CrashOutcome(int effectsBeforeCrash,
                               @Nullable StepStatus lastRecordBeforeCrash,
                               int effectsAfterRecovery,
                               List<Integer> retryingAttempts,
                               List<Integer> retryStartedAttempts,
                               int completedRecords,
                               boolean workflowReachedTerminal) {

    }

    /**
     * Observables of the external-cancel-during-retry-attempt drive.
     *
     * @param effectsBeforeCancel     action runs before the cancel (expected 2: attempt 1 timed out, attempt 2 running).
     * @param retryStartedBeforeCancel committed {@code RETRY_STARTED} records before the cancel (expected 1).
     * @param stepCancelRecorded      whether the step committed {@code CANCELLED} (expected {@code true}).
     * @param effectsAfterCancel      action runs after the cancel and another timeout window (expected unchanged, 2).
     * @param retryingRecords         committed {@code RETRYING} records at the end (expected 1: only attempt 1's).
     * @param retryStartedRecords     committed {@code RETRY_STARTED} records at the end (expected 1: only attempt 2's).
     * @param timedOutRecords         committed {@code TIMED_OUT} records at the end (expected 0).
     * @param workflowStatus          the workflow's last recorded status, for the record; the body's handling of an
     *                                uncaught {@code StepCancellationException} is not this scenario's oracle.
     */
    public record CancelOutcome(int effectsBeforeCancel,
                                int retryStartedBeforeCancel,
                                boolean stepCancelRecorded,
                                int effectsAfterCancel,
                                int retryingRecords,
                                int retryStartedRecords,
                                int timedOutRecords,
                                @Nullable WorkflowStatus workflowStatus) {

    }

    /**
     * Crashes the engine while attempt 2 of the fixed-backoff step is in flight and lets it recover.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
    public static CrashOutcome runCrashDuringRetryAttempt(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "retryedge-" + orderId;
        var step = RetryTimingWorkflow.STEP_FIXED;
        try (var world = new SimulationWorld(seed,
                                             EngineInstance.retryTimingRetryEdgesWorkflow(effects,
                                                                                          new OnRetryFires()))) {
            world.engine().publish(new RetryEdgesRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "attempt 1 of the fixed-backoff step to record RETRYING",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId, step,
                                                               StepStatus.RETRYING) >= 1);

            // Attempt 2 fails as well. Drop its RETRYING commit so the durable log ends at RETRY_STARTED(2): the
            // attempt is in flight as far as the log can tell, which is the state a crash must recover from.
            world.eventStore().armVanishCommitFor(step, StepStatus.RETRYING);
            Polling.awaitOrFail(DEADLINE, "attempt 2 to start and fail while its RETRYING commit vanishes", () -> {
                if (world.eventStore().isVanishArmed()) {
                    world.advanceTime(RetryTimingWorkflow.FIXED_DELAY);
                }
                return !world.eventStore().isVanishArmed();
            });
            int effectsBeforeCrash = effects.count(workflowId, step);
            StepStatus lastRecordBeforeCrash = latestStepStatus(world.committedLog(), workflowId, step).orElse(null);

            world.crashAndRecover();
            Polling.awaitOrFail(DEADLINE, "the recovered instance to reach a terminal workflow status", () -> {
                world.advanceTime(Duration.ofSeconds(1));
                return FenceOracles.terminalRecords(world.committedLog(), workflowId) >= 1;
            });

            List<EventMessage> log = world.committedLog();
            Invariants.assertRetryBound(log, Map.of(step, RetryTimingWorkflow.BACKOFF_MAX_RETRIES));
            Invariants.assertAtMostOnceRecording(log);
            return new CrashOutcome(effectsBeforeCrash,
                                    lastRecordBeforeCrash,
                                    effects.count(workflowId, step),
                                    attemptNumbers(log, workflowId, step, StepStatus.RETRYING),
                                    attemptNumbers(log, workflowId, step, StepStatus.RETRY_STARTED),
                                    FenceOracles.stepRecords(log, workflowId, step, StepStatus.COMPLETED),
                                    FenceOracles.terminalRecords(log, workflowId) >= 1);
        }
    }

    /**
     * Cancels the slow step externally while its retry attempt is running. Runs in aligned-clock mode so the
     * per-attempt timeout that ends attempt 1 is driven by an exact virtual advance and attempt 2 stays in flight.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
    public static CancelOutcome runExternalCancelDuringRetryAttempt(long seed, String orderId) {
        var effects = new CountingEffects();
        var latch = new CountDownLatch(1);
        var workflowId = "retrytimeout-" + orderId;
        var step = RetryTimingWorkflow.STEP_SLOW_EXECUTE;
        var registration = EngineInstance.retryTimingTimeoutEdgeWorkflow(effects, new OnRetryFires(), latch);
        try (var world = new SimulationWorld(seed, registration, true)) {
            world.engine().publish(new RetryTimeoutRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "attempt 1 of the slow step to start and block",
                                () -> effects.count(workflowId, step) >= 1);

            // One advance past attempt 1's window: it times out, RETRYING(1) is recorded, attempt 2 records
            // RETRY_STARTED(2) at the advanced clock and blocks again. No further advance until the cancel landed, so
            // attempt 2's own window (measured from its RETRY_STARTED) cannot elapse first.
            world.advanceTime(RetryTimingWorkflow.EXECUTE_TIMEOUT.plusSeconds(1));
            Polling.awaitOrFail(DEADLINE, "attempt 2 to record RETRY_STARTED and start running",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId, step,
                                                               StepStatus.RETRY_STARTED) >= 1
                                        && effects.count(workflowId, step) >= 2);
            int effectsBeforeCancel = effects.count(workflowId, step);
            int retryStartedBeforeCancel = FenceOracles.stepRecords(world.committedLog(), workflowId, step,
                                                                    StepStatus.RETRY_STARTED);

            // The external cancel is idempotent once the future is gone; repeat it until CANCELLED commits.
            boolean stepCancelRecorded = Polling.await(DEADLINE, () -> {
                world.engine().cancelRunningStepOf(workflowId, step, new StepCancellationException(
                        "cancelled externally during a retry attempt"));
                return FenceOracles.stepRecords(world.committedLog(), workflowId, step, StepStatus.CANCELLED) >= 1;
            });

            // Bounded absence: another timeout window must not produce a retry decision, a new attempt or a TIMED_OUT.
            Polling.await(ABSENCE_WINDOW, () -> {
                world.advanceTime(RetryTimingWorkflow.EXECUTE_TIMEOUT.plusSeconds(1));
                var log = world.committedLog();
                return FenceOracles.stepRecords(log, workflowId, step, StepStatus.RETRYING) > 1
                        || FenceOracles.stepRecords(log, workflowId, step, StepStatus.RETRY_STARTED) > 1
                        || FenceOracles.stepRecords(log, workflowId, step, StepStatus.TIMED_OUT) > 0;
            });
            Polling.await(ABSENCE_WINDOW,
                          () -> FenceOracles.terminalRecords(world.committedLog(), workflowId) >= 1);

            List<EventMessage> log = world.committedLog();
            return new CancelOutcome(effectsBeforeCancel,
                                     retryStartedBeforeCancel,
                                     stepCancelRecorded,
                                     effects.count(workflowId, step),
                                     FenceOracles.stepRecords(log, workflowId, step, StepStatus.RETRYING),
                                     FenceOracles.stepRecords(log, workflowId, step, StepStatus.RETRY_STARTED),
                                     FenceOracles.stepRecords(log, workflowId, step, StepStatus.TIMED_OUT),
                                     latestWorkflowStatus(log, workflowId).orElse(null));
        } finally {
            latch.countDown();
        }
    }

    // ---- log readers (per (workflowId, stepName), content-based) ----

    private static List<Integer> attemptNumbers(List<EventMessage> log, String workflowId, String stepName,
                                                StepStatus status) {
        return log.stream()
                  .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                          && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                          && MetadataUtils.getStepStatus(e.metadata()).filter(s -> s == status).isPresent())
                  .map(e -> e.payloadAs(Object.class))
                  .filter(StepRetryInfo.class::isInstance)
                  .map(p -> ((StepRetryInfo) p).attempt())
                  .toList();
    }

    private static Optional<StepStatus> latestStepStatus(List<EventMessage> log, String workflowId,
                                                         String stepName) {
        StepStatus latest = null;
        for (EventMessage e : log) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                    && stepName.equals(MetadataUtils.getStepName(e.metadata()))) {
                latest = MetadataUtils.getStepStatus(e.metadata()).orElse(latest);
            }
        }
        return Optional.ofNullable(latest);
    }

    private static Optional<WorkflowStatus> latestWorkflowStatus(List<EventMessage> log, String workflowId) {
        WorkflowStatus latest = null;
        for (EventMessage e : log) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))) {
                latest = MetadataUtils.getWorkflowStatus(e.metadata()).orElse(latest);
            }
        }
        return Optional.ofNullable(latest);
    }
}
