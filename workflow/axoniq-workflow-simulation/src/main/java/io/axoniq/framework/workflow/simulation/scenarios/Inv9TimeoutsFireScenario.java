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
import io.axoniq.framework.workflow.simulation.workflow.TimeoutWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.TimeoutRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Deterministic scenario for INVARIANTS.md INV-9 ({@code TimeoutsFire}): a step that exceeds its configured timeout
 * reaches a {@code TIMED_OUT} outcome (recorded) — a timeout never silently hangs or vanishes; the workflow always gets
 * a terminal step outcome it can act on.
 * <p>
 * Drives {@link TimeoutWorkflow}, whose {@code awaitConfirmation} step waits for a {@code PaymentConfirmedEvent} that is
 * never delivered, under a short {@link TimeoutWorkflow#AWAIT_TIMEOUT}. The timeout is driven the clean, fully-virtual
 * way the task prefers: the wait timeout is scheduled on the injectable {@code WorkflowScheduler} (the harness wires
 * {@code ManualWorkflowScheduler} / virtual time), so the scenario <strong>advances virtual time</strong> past the
 * timeout window and the engine's {@code WaitForDelegate} fires the scheduled continuation, recording the step's
 * {@code TIMED_OUT} event. The workflow then terminates (its body catches the surfaced timeout and calls
 * {@code ctx.fail}). INV-9 is DST-only (the Phase-2 TLA+ model is scoped to leasing + crash-recovery and explicitly
 * does not model time or timeouts — see INVARIANTS.md INV-9 "Checked by" and {@code formal/tla/README.md} "Scope &amp;
 * limits"). This is the timeout-path twin of {@code Inv8RetryBoundScenario} (retries) and {@code TerminalIsFinalScenario}
 * (cancel).
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body records {@code reserveInventory} then parks at {@code awaitConfirmation}
 *       (STARTED, with a scheduled wait-timeout);</li>
 *   <li>advance virtual time past {@link TimeoutWorkflow#AWAIT_TIMEOUT}: the scheduled timeout fires and records the
 *       step's {@code TIMED_OUT} event; assert {@link Invariants#assertTimeoutsFire} holds and the step's recorded status
 *       is {@code TIMED_OUT};</li>
 *   <li>crash + recover (drives the real replay path) <strong>alone</strong> — no event redelivered: the recorded
 *       terminal {@code awaitConfirmation} must replay as a cached result, launching no fresh wait and re-recording
 *       nothing, so the step's committed records are byte-for-byte unchanged. This mirrors {@code TerminalIsFinalScenario}
 *       / {@code Inv8RetryBoundScenario}'s in-scope crash+replay check.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class Inv9TimeoutsFireScenario {

    private Inv9TimeoutsFireScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal      whether the instance recorded a terminal workflow status.
     * @param stepStatusAtTimeout  the recorded status of the timing-out step once its timeout window elapsed (expected
     *                             {@code TIMED_OUT}).
     * @param stepRecordsAtTimeout number of committed events for the timing-out step once it timed out.
     * @param stepRecordsAfterCrash number of committed events for the timing-out step after a crash + replay
     *                             <strong>alone</strong> (no redelivery). INV-9 requires this to be unchanged from
     *                             {@code stepRecordsAtTimeout} — replay must not re-arm/re-record the already-timed-out
     *                             step.
     */
    public record Outcome(boolean reachedTerminal, StepStatus stepStatusAtTimeout, int stepRecordsAtTimeout,
                          int stepRecordsAfterCrash) {

    }

    /**
     * Runs the scenario against a fresh world (driving {@link TimeoutWorkflow}) and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.timeoutWorkflow(new CountingEffects());
        var timeouts = Map.of(TimeoutWorkflow.STEP_AWAIT_TIMEOUT, TimeoutWorkflow.AWAIT_TIMEOUT);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "timeout-" + orderId;

            // 1. Start the workflow: reserveInventory succeeds, awaitConfirmation parks STARTED with a scheduled timeout.
            world.engine().publish(new TimeoutRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "awaitConfirmation to reach STARTED",
                                () -> hasStep(world.committedLog(), workflowId, TimeoutWorkflow.STEP_AWAIT_TIMEOUT));
            // Wait until the wait-timeout continuation is actually registered on the (virtual-time) scheduler before
            // advancing time. WaitForDelegate schedules it via scheduler.delayedExecutor(remainingTimeout) only AFTER the
            // STARTED event is applied; advancing before it is queued would compute the next delay against the
            // already-advanced clock and the timer would land in the future (and never fire under a single advance).
            Polling.awaitOrFail(Duration.ofSeconds(10), "the wait-timeout to be scheduled on the virtual scheduler",
                                () -> world.scheduler().pendingTasks() > 0);

            // 2. Advance virtual time past the wait timeout — the clean, fully-virtual path: this fires the scheduled
            // timeout continuation, which records the step's TIMED_OUT event. (No PaymentConfirmedEvent is ever
            // delivered, so the only way the wait resolves is the timeout.)
            //
            // The advance amount is derived from the step's recorded STARTED timestamp rather than a fixed delta. The
            // engine computes the wait timeout's deadline as `step.timestamp() + AWAIT_TIMEOUT` (WaitForDelegate.java
            // line 114-116, anchored on the STARTED step record's own timestamp), and the virtual scheduler fires a
            // queued task only once its due time in epoch-millis is reached. Because the in-memory event store stamps
            // events from the Axon `GenericEventMessage` clock (the un-injectable static `Clock` residual — ARCHITECTURE
            // §12 / adoc D5) while the harness's MutableClock starts at the Unix epoch, the recorded STARTED timestamp
            // and the virtual clock can sit in different eras; advancing by a fixed 5s would never cross the deadline.
            // Reading the actual STARTED timestamp and advancing to `started + AWAIT_TIMEOUT + buffer` crosses it
            // deterministically (the recorded timestamp is a pure function of the committed log), with no global clock
            // mutation and entirely on the injectable scheduler.
            Instant started = startedAt(world.committedLog(), workflowId, TimeoutWorkflow.STEP_AWAIT_TIMEOUT)
                    .orElseThrow();
            Instant fireBy = started.plus(TimeoutWorkflow.AWAIT_TIMEOUT).plusSeconds(1);
            Duration advance = Duration.between(world.clock().instant(), fireBy);
            world.advanceTime(advance.isNegative() ? Duration.ZERO : advance);
            Polling.awaitOrFail(Duration.ofSeconds(10), "awaitConfirmation to reach a terminal (TIMED_OUT) step status",
                                () -> hasTerminalStep(world.committedLog(), workflowId,
                                                      TimeoutWorkflow.STEP_AWAIT_TIMEOUT));

            // INV-9 must hold: the step's window has elapsed and a TIMED_OUT record exists.
            Invariants.assertTimeoutsFire(world.committedLog(), timeouts, world.clock().instant());
            StepStatus stepStatus = latestStepStatus(world.committedLog(), workflowId,
                                                     TimeoutWorkflow.STEP_AWAIT_TIMEOUT).orElseThrow();
            int recordsAtTimeout = stepRecords(world.committedLog(), workflowId,
                                               TimeoutWorkflow.STEP_AWAIT_TIMEOUT);

            // 3. Crash + replay ALONE (no redelivery): the recorded terminal awaitConfirmation must replay as a cached
            // result, launching no fresh wait and recording nothing further — the step's records must be unchanged.
            world.crashAndRecover();
            // Give the recovered engine a bounded window to (incorrectly) re-arm/re-record the already-timed-out step.
            Polling.await(Duration.ofSeconds(2),
                          () -> stepRecords(world.committedLog(), workflowId, TimeoutWorkflow.STEP_AWAIT_TIMEOUT)
                                  > recordsAtTimeout);
            Invariants.assertTimeoutsFire(world.committedLog(), timeouts, world.clock().instant());
            int recordsAfterCrash = stepRecords(world.committedLog(), workflowId,
                                                TimeoutWorkflow.STEP_AWAIT_TIMEOUT);

            boolean reachedTerminal = isTerminalWorkflow(world.committedLog(), workflowId);
            return new Outcome(reachedTerminal, stepStatus, recordsAtTimeout, recordsAfterCrash);
        }
    }

    /**
     * Counts committed events for a {@code (workflowId, stepName)}.
     */
    private static int stepRecords(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return (int) committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent())
                .count();
    }

    /**
     * Returns the status of the latest committed step event for a {@code (workflowId, stepName)}, if any.
     */
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

    /**
     * Returns the committed STARTED timestamp of a {@code (workflowId, stepName)}, if any — the instant the engine
     * anchors the wait-timeout deadline on (it advances virtual time to {@code started + timeout} to fire the timeout).
     */
        private static Optional<Instant> startedAt(List<EventMessage> committedLog, String workflowId,
                                               String stepName) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == StepStatus.STARTED).orElse(false))
                .map(EventMessage::timestamp)
                .findFirst();
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent());
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
