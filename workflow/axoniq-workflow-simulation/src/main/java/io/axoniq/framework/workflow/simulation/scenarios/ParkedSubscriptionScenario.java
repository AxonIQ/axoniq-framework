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
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RenewalDecidedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SubscriptionStartedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SubscriptionRenewalWorkflow;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * Phase-2 production-realism scenarios: a <strong>day-scale parked</strong> {@link SubscriptionRenewalWorkflow}
 * surviving the things production does to long-lived instances — crash/recovery cycles, short-instance churn around
 * it, duplicate signals, duplicate start deliveries, and (deterministically) the elapse of its 30-day window.
 * <p>
 * The headline probe is the <strong>lost-wake crash window</strong> (hunted read-only, both hunters converging on
 * {@code SimpleWorkflowExecution.onEvent}'s mode asymmetry): the matching signal is delivered and matched LIVE, the
 * wait's COMPLETED commit vanishes in the crash, and on recovery the (durably committed) signal is re-delivered in
 * REPLAY mode — which only evolves state and never evaluates wait conditions — while the wait re-registers only when
 * the body re-runs at live-switch. Nothing ever re-matches the signal: the wake is permanently lost; only a brand-new
 * delivery (producer retry) or the wait's own timeout can move the instance again.
 * <p>
 * All observables are content-based and per-{@code workflowId}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ParkedSubscriptionScenario {

    /**
     * Bounded wall-clock deadline for poll steps.
     */
    private static final Duration DEADLINE = Duration.ofSeconds(10);

    /**
     * Bounded observation window for conditions expected to stay ABSENT (the parked / no-wake assertions).
     */
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(3);

    /**
     * Per-nudge virtual-time advance used to flush churn timers (OrderWorkflow's 1s sleep + 200ms retry backoffs).
     */
    private static final Duration CHURN_NUDGE = Duration.ofMillis(500);

    /**
     * Maximum churn nudges per poll before failing loudly.
     */
    private static final int MAX_NUDGES = 30;

    private ParkedSubscriptionScenario() {
    }

    /**
     * Outcome of the lost-wake crash-window probe.
     *
     * @param signalCommittedBeforeCrash whether the matching {@link RenewalDecidedEvent} was durably committed before
     *                                   the crash (it must be — the producer's commit succeeded; only the wait step's
     *                                   COMPLETED vanished).
     * @param wokeAfterRecoveryAlone     whether recovery alone (replaying the committed signal) completed the wait —
     *                                   {@code false} under the gap: the replayed signal is never evaluated against
     *                                   the re-registered wait condition.
     * @param renewalEffectsAfterRecovery {@code processRenewal} effect executions after recovery alone (0 under the
     *                                   gap — the instance is still parked).
     * @param completedAfterRedelivery   whether a brand-new delivery of the same signal (the producer-retry rescue)
     *                                   woke the instance and drove it to COMPLETED.
     * @param waitCompletedRecords       committed COMPLETED records for the wait step at the end (exactly 1 — the
     *                                   redelivery wake; the vanished pre-crash one never reached the log).
     */
    public record LostWakeOutcome(boolean signalCommittedBeforeCrash, boolean wokeAfterRecoveryAlone,
                                  int renewalEffectsAfterRecovery, boolean completedAfterRedelivery,
                                  int waitCompletedRecords) {

    }

    /**
     * Terminal snapshot of the parked instance for the healthy-path probes.
     *
     * @param terminalStatus       the workflow's terminal status, or {@code null}.
     * @param waitCompletedRecords committed COMPLETED records for the wait step.
     * @param waitTimedOutRecords  committed TIMED_OUT records for the wait step.
     * @param startedRecords       committed {@code <workflow>:STARTED} records for the instance.
     * @param registerEffects      {@code registerSubscription} effect executions.
     * @param renewalEffects       {@code processRenewal} effect executions.
     * @param expireEffects        {@code expireSubscription} effect executions.
     */
    public record ParkedOutcome(@Nullable WorkflowStatus terminalStatus, int waitCompletedRecords,
                                int waitTimedOutRecords, int startedRecords, int registerEffects, int renewalEffects,
                                int expireEffects) {

    }

    /**
     * The lost-wake crash window: park the instance, deliver the matching renewal decision LIVE (it matches and the
     * wait's COMPLETED publish is armed to vanish), crash + recover, and observe whether the durably-committed signal
     * ever wakes the recovered instance — then prove the producer-retry rescue (a brand-new delivery) does.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code subs-<orderId>}).
     * @return the observed outcome.
     */
        public static LostWakeOutcome lostWakeOnCrashBetweenMatchAndCommit(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.subscriptionRenewalWorkflow(effects))) {
            String workflowId = "subs-" + orderId;

            world.engine().publish(new SubscriptionStartedEvent(orderId));
            awaitParked(world, workflowId);

            // Arm the crash window on the WAIT step's COMPLETED, then deliver the matching decision: it matches LIVE
            // (condition removed, completion task runs) and the COMPLETED commit vanishes — the signal itself is
            // durably committed by the producer, but the wait is left STARTED in the durable log.
            world.eventStore().armVanishCommitFor(SubscriptionRenewalWorkflow.STEP_AWAIT_DECISION,
                                                  StepStatus.COMPLETED);
            world.engine().publish(new RenewalDecidedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the wait's COMPLETED commit to vanish",
                                () -> !world.eventStore().isVanishArmed());
            // The signal is an EXTERNAL event (no workflowId metadata): it lives in the FULL durable log
            // (committedTaggedEvents), not in the workflow-events-only committedLog() view.
            boolean signalCommitted = signalCommitted(world, orderId);

            var beforeCrash = List.copyOf(world.committedLog());
            world.crashAndRecover();
            Invariants.assertCommittedHistorySurvivesCrash(beforeCrash, world.committedLog());

            // Recovery alone: the committed signal is re-delivered in REPLAY mode (state evolve only, no wait
            // evaluation); the wait re-registers when the body re-runs at live-switch — and nothing re-matches the
            // signal. Bounded absence window: the wake must NOT happen for the gap to be confirmed.
            Polling.await(ABSENCE_WINDOW,
                          () -> waitStatusRecords(world.committedLog(), workflowId, StepStatus.COMPLETED) > 0);
            boolean wokeAfterRecovery =
                    waitStatusRecords(world.committedLog(), workflowId, StepStatus.COMPLETED) > 0;
            int renewalAfterRecovery = effects.count(workflowId, SubscriptionRenewalWorkflow.STEP_PROCESS_RENEWAL);

            // The producer-retry rescue: a brand-new delivery is evaluated LIVE against the re-registered wait.
            world.engine().publish(new RenewalDecidedEvent(orderId));
            Polling.await(DEADLINE,
                          () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                      WorkflowStatus.COMPLETED) >= 1);
            boolean completedAfterRedelivery =
                    workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.COMPLETED) >= 1;

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new LostWakeOutcome(signalCommitted, wokeAfterRecovery, renewalAfterRecovery,
                                       completedAfterRedelivery,
                                       waitStatusRecords(world.committedLog(), workflowId, StepStatus.COMPLETED));
        }
    }

    /**
     * The healthy long-park: the instance parks while short order instances churn to completion around it across two
     * crash/recovery cycles; the renewal decision then arrives (twice — a duplicate delivery) and must wake the
     * instance exactly once.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the parked instance.
     * @return the observed outcome.
     */
        public static ParkedOutcome parkedSurvivesChurnRestartsAndDuplicateSignals(long seed, String orderId) {
        var effects = new CountingEffects();
        var registrations = List.of(EngineInstance.subscriptionRenewalWorkflow(effects),
                                    EngineInstance.orderWorkflow(effects));
        try (var world = new SimulationWorld(seed, registrations)) {
            String workflowId = "subs-" + orderId;

            world.engine().publish(new SubscriptionStartedEvent(orderId));
            awaitParked(world, workflowId);

            // Churn wave 1: two short orders run to completion around the parked instance.
            runOrderToCompletion(world, "churn1");
            runOrderToCompletion(world, "churn2");
            world.crashAndRecover();

            // Churn wave 2 after recovery, then a second recovery — the parked instance re-parks each time.
            runOrderToCompletion(world, "churn3");
            world.crashAndRecover();

            // The recovered engine must still hold the parked instance (restored from the running-workflows record).
            Polling.awaitOrFail(DEADLINE, "the parked instance to be LIVE in the recovered engine",
                                () -> world.engine().liveWorkflowIds().contains(workflowId));

            // The decision arrives twice (a duplicate delivery, as flaky transports do). The FIRST delivery is also what
            // closes the recovered segment's catch-up (F-40): a restored body starts only once its segment sees a
            // delivered event whose token covers the startup head, and after this crash the log's tail belongs to the
            // churn instance's segment, not this one. So the first decision starts the body, which re-registers the
            // wait and reschedules its timeout; the same delivery may then wake it at once, so the resume is observed
            // either as the rescheduled timeout or as the completed wait.
            world.engine().publish(new RenewalDecidedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the recovered body to resume (wait re-registered, or already woken)",
                                () -> world.scheduler().pendingTasks() > 0
                                        || hasStepRecord(world.committedLog(), workflowId,
                                                         SubscriptionRenewalWorkflow.STEP_AWAIT_DECISION,
                                                         StepStatus.COMPLETED));
            world.engine().publish(new RenewalDecidedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the parked instance to complete after the (duplicated) decision",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return snapshot(world, effects, workflowId);
        }
    }

    /**
     * Timer fidelity across restarts: the parked instance survives two crash/recovery cycles (each re-registering the
     * wait with the REMAINING window, recomputed from the recorded STARTED timestamp), then the 30-day window is
     * elapsed deterministically — the timeout must fire exactly once and drive the expiry path.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the parked instance.
     * @return the observed outcome.
     */
        public static ParkedOutcome timeoutFiresOnceAcrossRestarts(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.subscriptionRenewalWorkflow(effects))) {
            String workflowId = "subs-" + orderId;

            world.engine().publish(new SubscriptionStartedEvent(orderId));
            awaitParked(world, workflowId);

            world.crashAndRecover();
            awaitParked(world, workflowId);
            world.crashAndRecover();
            awaitParked(world, workflowId);

            // Elapse the 30-day window (era-anchored on the recorded STARTED timestamp, the Inv9 precedent).
            Polling.awaitOrFail(DEADLINE, "the re-registered wait-timeout to be scheduled",
                                () -> world.scheduler().pendingTasks() > 0);
            Instant started = startedAt(world.committedLog(), workflowId,
                                        SubscriptionRenewalWorkflow.STEP_AWAIT_DECISION)
                    .orElseThrow(() -> new IllegalStateException("awaitRenewalDecision has no STARTED record"));
            Instant fireBy = started.plus(SubscriptionRenewalWorkflow.DECISION_TIMEOUT).plusSeconds(1);
            Duration advance = Duration.between(world.clock().instant(), fireBy);
            world.advanceTime(advance.isNegative() ? Duration.ZERO : advance);

            Polling.awaitOrFail(DEADLINE, "the expiry path to cancel the subscription",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.CANCELLED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return snapshot(world, effects, workflowId);
        }
    }

    /**
     * A duplicate-start storm against the LIVE parked id: the same start event is redelivered five times while the
     * instance is parked — spawn dedup must hold (one instance, one {@code <workflow>:STARTED}, one register effect),
     * and the decision must then complete the single instance normally.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the parked instance.
     * @return the observed outcome.
     */
        public static ParkedOutcome duplicateStartStormOnLiveParkedId(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.subscriptionRenewalWorkflow(effects))) {
            String workflowId = "subs-" + orderId;

            world.engine().publish(new SubscriptionStartedEvent(orderId));
            awaitParked(world, workflowId);

            for (int i = 0; i < 5; i++) {
                world.engine().publish(new SubscriptionStartedEvent(orderId));
            }
            // Bounded absence window: no second STARTED / no register re-run may appear.
            Polling.await(ABSENCE_WINDOW,
                          () -> workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.STARTED) > 1);

            world.engine().publish(new RenewalDecidedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the parked instance to complete after the decision",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return snapshot(world, effects, workflowId);
        }
    }

    /**
     * Waits until the instance is parked: its wait step is STARTED with no terminal record yet.
     */
    private static void awaitParked(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(DEADLINE, "instance " + workflowId + " to park on awaitRenewalDecision",
                            () -> hasStepRecord(world.committedLog(), workflowId,
                                                SubscriptionRenewalWorkflow.STEP_AWAIT_DECISION, StepStatus.STARTED));
    }

    /**
     * Drives one {@link OrderWorkflow} churn instance to COMPLETED: start, confirm payment, nudge virtual time
     * through its settle sleep and ship retries.
     */
    private static void runOrderToCompletion(SimulationWorld world, String churnOrderId) {
        String churnWorkflowId = "order-" + churnOrderId;
        world.engine().publish(new OrderPlacedEvent(churnOrderId));
        Polling.awaitOrFail(DEADLINE, "churn order " + churnOrderId + " to reach its confirmation wait",
                            () -> hasStepRecord(world.committedLog(), churnWorkflowId,
                                                OrderWorkflow.STEP_AWAIT_CONFIRMATION, StepStatus.STARTED));
        world.engine().publish(new PaymentConfirmedEvent(churnOrderId));
        advanceUntilOrFail(world, "churn order " + churnOrderId + " to COMPLETE",
                           () -> workflowStatusRecords(world.committedLog(), churnWorkflowId,
                                                       WorkflowStatus.COMPLETED) >= 1);
    }

    /**
     * Polls the condition under bounded virtual-time nudges (flushing sleep/backoff timers), failing loudly when the
     * budget is exhausted.
     */
    private static void advanceUntilOrFail(SimulationWorld world, String description,
                                           BooleanSupplier condition) {
        for (int i = 0; i < MAX_NUDGES && !condition.getAsBoolean(); i++) {
            world.advanceTime(CHURN_NUDGE);
            Polling.await(Duration.ofMillis(300), condition);
        }
        Polling.awaitOrFail(Duration.ofSeconds(5), description, condition);
    }

    /**
     * Builds the terminal snapshot for the parked instance.
     */
        private static ParkedOutcome snapshot(SimulationWorld world, CountingEffects effects,
                                          String workflowId) {
        return new ParkedOutcome(
                terminalWorkflowStatus(world.committedLog(), workflowId),
                waitStatusRecords(world.committedLog(), workflowId, StepStatus.COMPLETED),
                waitStatusRecords(world.committedLog(), workflowId, StepStatus.TIMED_OUT),
                workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.STARTED),
                effects.count(workflowId, SubscriptionRenewalWorkflow.STEP_REGISTER),
                effects.count(workflowId, SubscriptionRenewalWorkflow.STEP_PROCESS_RENEWAL),
                effects.count(workflowId, SubscriptionRenewalWorkflow.STEP_EXPIRE));
    }

    /**
     * Whether the matching {@link RenewalDecidedEvent} for {@code orderId} is present in the FULL durable log
     * (external events carry no workflowId metadata, so they are filtered out of the workflow-events-only
     * {@code committedLog()} view — but they ARE durable and ARE carried across recovery).
     */
    private static boolean signalCommitted(SimulationWorld world, String orderId) {
        return world.eventStore().committedTaggedEvents().stream()
                    .map(t -> t.event())
                    .anyMatch(e -> String.valueOf(e.type()).contains("RenewalDecided")
                            || (e.payload() instanceof RenewalDecidedEvent r && orderId.equals(r.orderId())));
    }

    /**
     * How many records of the wait step with {@code status} the instance's committed log holds.
     */
    private static int waitStatusRecords(List<EventMessage> committedLog, String workflowId,
                                         StepStatus status) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> SubscriptionRenewalWorkflow.STEP_AWAIT_DECISION
                                         .equals(MetadataUtils.getStepName(e.metadata())))
                                 .filter(e -> MetadataUtils.getStepStatus(e.metadata())
                                                           .map(status::equals).orElse(false))
                                 .count();
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
