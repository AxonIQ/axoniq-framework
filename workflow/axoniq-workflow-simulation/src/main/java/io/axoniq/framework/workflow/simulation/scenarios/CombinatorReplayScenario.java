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
import io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BranchASignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BranchBSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BranchCSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorReplayRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.GateOpenedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Combinator replay-determinism probes (see {@link CombinatorReplayWorkflow}): drive each combinator through a live
 * pass whose branch states CHANGE before a crash, recover, and compare what the recovered re-run observed against the
 * live pass — the per-run snapshot counters make the comparison direct (a key at count 2 was seen by BOTH passes; a
 * key at count 1 was seen by exactly one — the divergence observable).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class CombinatorReplayScenario {

    /**
     * Bounded wall-clock deadline for poll steps.
     */
    private static final Duration DEADLINE = Duration.ofSeconds(10);

    private CombinatorReplayScenario() {
    }

    /**
     * Outcome of a probe run: the snapshot counters keyed by their effect name (without the workflowId prefix), plus
     * the run-pass count and the instance's terminal status.
     *
     * @param runs           body run passes recorded (2 = live + one recovered re-run).
     * @param counters       every snapshot counter (matched:/unmatched:/verdict:/winner: keys) and its count.
     * @param terminalStatus the instance's terminal workflow status at the end.
     */
    public record Outcome(int runs, Map<String, Integer> counters, WorkflowStatus terminalStatus) {

    }

    /**
     * The user-posed scenario: {@code allMatch} short-circuits on a failed branch while branch C is still in flight;
     * C then completes BEFORE the crash; the recovered re-run recomputes the categories over the now-cached states.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code creplay-<orderId>}).
     * @return the observed outcome.
     */
        public static Outcome allMatchLateCompletionAcrossCrash(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.combinatorReplayWorkflow(effects))) {
            String workflowId = "creplay-" + orderId;

            world.engine().publish(new CombinatorReplayRequestedEvent(orderId,
                                                                      CombinatorReplayWorkflow.MODE_ALL_MATCH_LATE));
            // The live pass: A COMPLETED, B FAILED, the allMatch short-circuited and the snapshot landed (C still
            // in flight at snapshot time).
            Polling.awaitOrFail(DEADLINE, "the live allMatch snapshot to land",
                                () -> effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_VERDICT_PREFIX
                                        + "false") >= 1);

            // C completes AFTER the live short-circuit, BEFORE the crash.
            world.engine().publish(new BranchCSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "branchC to complete before the crash",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    CombinatorReplayWorkflow.STEP_BRANCH_C, StepStatus.COMPLETED));

            crashRecoverAndAwaitSecondRun(world, effects, workflowId);
            releaseGateAndAwaitCompletion(world, workflowId, orderId);

            return outcome(world, effects, workflowId);
        }
    }

    /**
     * {@code anyMatch} winner identity across replay: B completes first live (the winner); A completes after; the
     * recovered re-run recomputes the winner over BOTH cached completions from the durable first-completed order.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome anyMatchWinnerAcrossCrash(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.combinatorReplayWorkflow(effects))) {
            String workflowId = "creplay-" + orderId;

            world.engine().publish(new CombinatorReplayRequestedEvent(orderId,
                                                                      CombinatorReplayWorkflow.MODE_ANY_MATCH_WINNER));
            Polling.awaitOrFail(DEADLINE, "both branch waits to park",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    CombinatorReplayWorkflow.STEP_BRANCH_A, StepStatus.STARTED)
                                        && hasStepRecord(world.committedLog(), workflowId,
                                                         CombinatorReplayWorkflow.STEP_BRANCH_B, StepStatus.STARTED));

            // B wins live...
            world.engine().publish(new BranchBSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the live anyMatch winner to land",
                                () -> effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_WINNER_PREFIX
                                        + CombinatorReplayWorkflow.STEP_BRANCH_B) >= 1);
            // ...then A completes too, so the recovered recompute sees BOTH cached COMPLETED.
            world.engine().publish(new BranchASignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "branchA to complete before the crash",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    CombinatorReplayWorkflow.STEP_BRANCH_A, StepStatus.COMPLETED));

            crashRecoverAndAwaitSecondRun(world, effects, workflowId);
            releaseGateAndAwaitCompletion(world, workflowId, orderId);

            return outcome(world, effects, workflowId);
        }
    }

    /**
     * {@code noneMatch(failure)} verdict over a COMPLETED + TIMED_OUT pair (the C-2 foot-gun), and its replay
     * stability across a crash.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome noneMatchTimedOutBranchAcrossCrash(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.combinatorReplayWorkflow(effects))) {
            String workflowId = "creplay-" + orderId;

            world.engine().publish(new CombinatorReplayRequestedEvent(orderId,
                                                                      CombinatorReplayWorkflow.MODE_NONE_MATCH_TIMEOUT));
            Polling.awaitOrFail(DEADLINE, "the timed branch to park",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    CombinatorReplayWorkflow.STEP_BRANCH_B, StepStatus.STARTED));

            // Elapse the timed branch's window (era-anchored on its recorded STARTED — the Inv9 precedent).
            Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled",
                                () -> world.scheduler().pendingTasks() > 0);
            Instant started = startedAt(world.committedLog(), workflowId, CombinatorReplayWorkflow.STEP_BRANCH_B)
                    .orElseThrow(() -> new IllegalStateException("timed branch has no STARTED record"));
            Instant fireBy = started.plus(CombinatorReplayWorkflow.NONE_MATCH_WAIT_TIMEOUT).plusSeconds(1);
            Duration advance = Duration.between(world.clock().instant(), fireBy);
            world.advanceTime(advance.isNegative() ? Duration.ZERO : advance);

            Polling.awaitOrFail(DEADLINE, "the live noneMatch verdict to land",
                                () -> effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_RUN) >= 1
                                        && totalVerdicts(effects, workflowId) >= 1);

            crashRecoverAndAwaitSecondRun(world, effects, workflowId);
            releaseGateAndAwaitCompletion(world, workflowId, orderId);

            return outcome(world, effects, workflowId);
        }
    }

    private static void crashRecoverAndAwaitSecondRun(SimulationWorld world,
                                                      CountingEffects effects,
                                                      String workflowId) {
        world.crashAndRecover();
        Polling.awaitOrFail(DEADLINE, "the recovered re-run to record its snapshot",
                            () -> effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_RUN) >= 2);
        // F-16 discipline: the gate wait must be re-registered before its release is delivered.
        Polling.awaitOrFail(DEADLINE, "the recovered gate wait to be re-registered",
                            () -> world.scheduler().pendingTasks() > 0);
    }

    private static void releaseGateAndAwaitCompletion(SimulationWorld world, String workflowId,
                                                      String orderId) {
        world.engine().publish(new GateOpenedEvent(orderId));
        Polling.awaitOrFail(DEADLINE, "the instance to complete after the gate release",
                            () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                        WorkflowStatus.COMPLETED) >= 1);
    }

        private static Outcome outcome(SimulationWorld world, CountingEffects effects,
                                   String workflowId) {
        Map<String, Integer> counters = new TreeMap<>();
        String prefix = workflowId + "/";
        effects.snapshot().forEach((key, count) -> {
            if (key.startsWith(prefix) && !key.endsWith("/" + CombinatorReplayWorkflow.EFFECT_RUN)) {
                counters.put(key.substring(prefix.length()), count);
            }
        });
        return new Outcome(effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_RUN), counters,
                           terminalWorkflowStatus(world.committedLog(), workflowId));
    }

    private static int totalVerdicts(CountingEffects effects, String workflowId) {
        return effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_VERDICT_PREFIX + "true")
                + effects.count(workflowId, CombinatorReplayWorkflow.EFFECT_VERDICT_PREFIX + "false");
    }

    private static boolean hasStepRecord(List<EventMessage> committedLog, String workflowId,
                                         String stepName, StepStatus status) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .anyMatch(e -> stepName.equals(MetadataUtils.getStepName(e.metadata()))
                                   && MetadataUtils.getStepStatus(e.metadata()).map(status::equals).orElse(false));
    }

    private static int workflowStatusRecords(List<EventMessage> committedLog, String workflowId,
                                             WorkflowStatus status) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                                           .map(status::equals).orElse(false))
                                 .count();
    }

    private static WorkflowStatus terminalWorkflowStatus(List<EventMessage> committedLog,
                                                         String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .map(e -> MetadataUtils.getWorkflowStatus(e.metadata()).orElse(null))
                           .filter(s -> s != null && s.isTerminal())
                           .findFirst()
                           .orElse(null);
    }

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
