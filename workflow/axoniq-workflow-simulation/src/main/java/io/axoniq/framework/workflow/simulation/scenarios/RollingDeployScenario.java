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
import io.axoniq.framework.workflow.simulation.workflow.RollingDeployWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ApprovalGrantedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RollingDeployOrderEvent;
import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Phase-3 production-realism scenarios: <strong>the registry changes across recoveries while instances are parked
 * mid-flight</strong> — the rolling deploy. Three ops stories, each a deterministic run over the
 * {@link RollingDeployWorkflow}:
 * <ol>
 *   <li><strong>The recommended deploy</strong> — v2 added alongside v1: the parked v1 instance resumes on the v1
 *       body (closest-sibling routing), fresh starts spawn at v2 (with the {@code migrateVersion}-gated new step);</li>
 *   <li><strong>Premature v1 removal</strong> — only the correctly-authored v2 remains: the parked v1 instance routes
 *       to v2 via the closest-higher pass and forks mid-flight through the {@code migrateVersion} gate (the intended
 *       ADR-005 rescue);</li>
 *   <li><strong>The bad deploy + rollback</strong> — the structural change shipped WITHOUT {@code migrateVersion}:
 *       the drift guard's unreferenced-TERMINAL-step predicate has nothing to trip on for an instance parked on a
 *       non-terminal wait, so the new step runs SILENTLY (the blind spot); the new step's terminal record then
 *       poisons the rollback — the restored v1 body trips the guard on the now-unreferenced {@code fraudCheck}
 *       COMPLETED, pausing the instance mid-completion; only rolling FORWARD again releases it.</li>
 * </ol>
 * Version observables follow the INV-11/20 style: the set of distinct {@code MessageType.version()} values an
 * instance's committed events carry, plus the {@code migrateVersion} marker presence — a mixed-version history WITH a
 * marker is the designed fork; one WITHOUT a marker is the bad deploy's silent mutation.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class RollingDeployScenario {

    /**
     * Bounded wall-clock deadline for poll steps.
     */
    private static final Duration DEADLINE = Duration.ofSeconds(10);

    /**
     * Bounded observation window for conditions expected to stay ABSENT (pause / no-terminal assertions).
     */
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(3);

    private RollingDeployScenario() {
    }

    /**
     * Outcome of the recommended-deploy run.
     *
     * @param parkedTerminalStatus terminal status of the pre-deploy (v1) instance.
     * @param parkedFraudChecks    {@code fraudCheck} effect executions for the v1 instance — 0: it resumed on v1.
     * @param parkedMarkers        migration markers recorded by the v1 instance — 0.
     * @param parkedVersions       distinct event versions the v1 instance's records carry — exactly {v1}.
     * @param freshTerminalStatus  terminal status of the post-deploy (v2) instance.
     * @param freshFraudChecks     {@code fraudCheck} effect executions for the v2 instance — 1.
     * @param freshMarkers         migration markers recorded by the v2 instance — 1 (the gate records on first run).
     * @param freshVersions        distinct event versions the fresh instance's records carry.
     */
    public record RecommendedDeployOutcome(@Nullable WorkflowStatus parkedTerminalStatus, int parkedFraudChecks,
                                           long parkedMarkers, Set<String> parkedVersions,
                                           @Nullable WorkflowStatus freshTerminalStatus, int freshFraudChecks,
                                           long freshMarkers, Set<String> freshVersions) {

    }

    /**
     * Outcome of the premature-removal run.
     *
     * @param terminalStatus terminal status of the v1-recorded instance recovered under the v2-only registry.
     * @param fraudChecks    {@code fraudCheck} effect executions — 1: the mid-flight fork ran the gated step.
     * @param markers        migration markers — 1: the fork was recorded.
     * @param fulfils        {@code fulfillOrder} effect executions — 1.
     * @param versions       distinct event versions across the instance's records (mixed WITH a marker = the designed
     *                       ADR-005 fork).
     */
    public record PrematureRemovalOutcome(@Nullable WorkflowStatus terminalStatus, int fraudChecks, long markers,
                                          int fulfils, Set<String> versions) {

    }

    /**
     * Outcome of the bad-deploy + rollback + roll-forward run.
     *
     * @param fraudCheckRanSilently     whether the un-gated new step executed on the recovered parked instance with NO
     *                                  drift pause and NO migration marker (the blind spot).
     * @param markersAfterBadDeploy     migration markers after the bad deploy — 0 (nothing recorded the fork).
     * @param versionsAfterBadDeploy    distinct event versions after the bad deploy — mixed WITHOUT a marker (the
     *                                  silent mutation observable).
     * @param terminalAfterRollback     terminal status after the rollback + approval — {@code null}: the restored v1
     *                                  body trips the drift guard on the unreferenced {@code fraudCheck} COMPLETED and
     *                                  pauses the instance mid-completion (approval consumed, fulfillment withheld).
     * @param fulfilsAfterRollback      {@code fulfillOrder} effect executions after rollback + approval — 0.
     * @param waitCompletedAfterRollback whether the approval WAS consumed (the wait step records COMPLETED) before the
     *                                  pause — the instance holds a consumed approval it cannot act on.
     * @param liveAfterDriftPause       whether the drift-paused instance is still LIVE in the engine after the pause
     *                                  ({@code true}: a non-terminal exit keeps the instance registered for recovery).
     * @param restoredByRollForward     whether rolling forward (to the body that matches the polluted history)
     *                                  restored the instance.
     * @param terminalAfterRollForward  terminal status after the roll-forward ({@code COMPLETED}: the matching body
     *                                  replays cleanly and runs the withheld fulfillment).
     * @param fraudChecksTotal          total {@code fraudCheck} effect executions across the whole run (1).
     * @param fulfilsTotal              total {@code fulfillOrder} effect executions across the whole run (1).
     * @param completedRecords          committed {@code <workflow>:COMPLETED} records, after one further restart (1).
     * @param liveAtEnd                 whether the instance is still live after that further restart ({@code false}).
     */
    public record BadDeployOutcome(boolean fraudCheckRanSilently, long markersAfterBadDeploy,
                                   Set<String> versionsAfterBadDeploy, @Nullable WorkflowStatus terminalAfterRollback,
                                   int fulfilsAfterRollback, boolean waitCompletedAfterRollback,
                                   boolean liveAfterDriftPause, boolean restoredByRollForward,
                                   @Nullable WorkflowStatus terminalAfterRollForward, int fraudChecksTotal,
                                   int fulfilsTotal, int completedRecords, boolean liveAtEnd) {

    }

    /**
     * Ops story 1 — the recommended deploy: park A on v1, recover under [v1+v2], start B (spawns at v2), approve both.
     *
     * @param seed seed for the world's deterministic id source.
     * @return the observed outcome.
     */
        public static RecommendedDeployOutcome recommendedDeploy(long seed) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.rollingDeployWorkflowV1(effects))) {
            String parkedId = "deploy-A";
            String freshId = "deploy-B";

            world.engine().publish(new RollingDeployOrderEvent("A"));
            awaitParked(world, parkedId);

            // The deploy: v2 added alongside v1 (same effects registry so counts survive the swap).
            world.crashAndRecoverWith(EngineInstance.rollingDeployWorkflowV1V2(effects));
            awaitReParked(world, parkedId);

            // Traffic resumes: a fresh order spawns at the highest version (v2).
            world.engine().publish(new RollingDeployOrderEvent("B"));
            awaitParked(world, freshId);

            world.engine().publish(new ApprovalGrantedEvent("A"));
            world.engine().publish(new ApprovalGrantedEvent("B"));
            Polling.awaitOrFail(DEADLINE, "both instances to complete after approval",
                                () -> workflowStatusRecords(world.committedLog(), parkedId,
                                                            WorkflowStatus.COMPLETED) >= 1
                                        && workflowStatusRecords(world.committedLog(), freshId,
                                                                 WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new RecommendedDeployOutcome(
                    terminalWorkflowStatus(world.committedLog(), parkedId),
                    effects.count(parkedId, RollingDeployWorkflow.STEP_FRAUD_CHECK),
                    markerCount(world.committedLog(), parkedId),
                    distinctVersions(world.committedLog(), parkedId),
                    terminalWorkflowStatus(world.committedLog(), freshId),
                    effects.count(freshId, RollingDeployWorkflow.STEP_FRAUD_CHECK),
                    markerCount(world.committedLog(), freshId),
                    distinctVersions(world.committedLog(), freshId));
        }
    }

    /**
     * Ops story 2 — premature v1 removal: park A on v1, recover under [v2 only] (correctly authored), approve.
     *
     * @param seed seed for the world's deterministic id source.
     * @return the observed outcome.
     */
        public static PrematureRemovalOutcome prematureV1Removal(long seed) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.rollingDeployWorkflowV1(effects))) {
            String workflowId = "deploy-A";

            world.engine().publish(new RollingDeployOrderEvent("A"));
            awaitParked(world, workflowId);

            // The deploy that dropped v1: the recovered instance routes to v2 (closest-higher) and forks mid-flight
            // through the migrateVersion gate — the new step runs BEFORE re-parking.
            world.crashAndRecoverWith(List.of(EngineInstance.rollingDeployWorkflowV2Only(effects)));
            Polling.awaitOrFail(DEADLINE, "the mid-flight fork to run the gated fraudCheck",
                                () -> effects.count(workflowId, RollingDeployWorkflow.STEP_FRAUD_CHECK) >= 1);
            awaitReParked(world, workflowId);

            world.engine().publish(new ApprovalGrantedEvent("A"));
            Polling.awaitOrFail(DEADLINE, "the forked instance to complete after approval",
                                () -> workflowStatusRecords(world.committedLog(), workflowId,
                                                            WorkflowStatus.COMPLETED) >= 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new PrematureRemovalOutcome(
                    terminalWorkflowStatus(world.committedLog(), workflowId),
                    effects.count(workflowId, RollingDeployWorkflow.STEP_FRAUD_CHECK),
                    markerCount(world.committedLog(), workflowId),
                    effects.count(workflowId, RollingDeployWorkflow.STEP_FULFILL),
                    distinctVersions(world.committedLog(), workflowId));
        }
    }

    /**
     * Ops story 3 — the bad deploy, its silent acceptance, the poisoned rollback, and the roll-forward release.
     *
     * @param seed seed for the world's deterministic id source.
     * @return the observed outcome.
     */
        public static BadDeployOutcome badDeployThenRollback(long seed) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.rollingDeployWorkflowV1(effects))) {
            String workflowId = "deploy-A";

            world.engine().publish(new RollingDeployOrderEvent("A"));
            awaitParked(world, workflowId);

            // 1. The BAD deploy (structural change, no migrateVersion): the parked instance's only post-insertion
            // record is its non-terminal wait, so the drift guard has no unreferenced TERMINAL step to trip on — the
            // new step executes silently and the instance re-parks.
            world.crashAndRecoverWith(List.of(EngineInstance.rollingDeployWorkflowV2BadOnly(effects)));
            Polling.awaitOrFail(DEADLINE, "the un-gated fraudCheck to run silently on the recovered parked instance",
                                () -> effects.count(workflowId, RollingDeployWorkflow.STEP_FRAUD_CHECK) >= 1);
            awaitReParked(world, workflowId);
            boolean ranSilently = terminalWorkflowStatus(world.committedLog(), workflowId) == null
                    && effects.count(workflowId, RollingDeployWorkflow.STEP_FRAUD_CHECK) == 1;
            long markersAfterBad = markerCount(world.committedLog(), workflowId);
            Set<String> versionsAfterBad = distinctVersions(world.committedLog(), workflowId);

            // 2. The ROLLBACK: v1 restored. Replay re-parks cleanly (reserve referenced, wait still STARTED) — but on
            // the wake, the v1 body's fulfill step trips guardAgainstReplayDrift on the now-unreferenced fraudCheck
            // COMPLETED: the instance pauses NON-terminally, mid-completion, holding a consumed approval.
            world.crashAndRecoverWith(List.of(EngineInstance.rollingDeployWorkflowV1(effects)));
            awaitReParked(world, workflowId);
            world.engine().publish(new ApprovalGrantedEvent("A"));
            Polling.awaitOrFail(DEADLINE, "the approval to be consumed (wait COMPLETED) before the drift pause",
                                () -> hasStepRecord(world.committedLog(), workflowId,
                                                    RollingDeployWorkflow.STEP_AWAIT_APPROVAL, StepStatus.COMPLETED));
            // Bounded absence window: NO terminal may land (the pause).
            Polling.await(ABSENCE_WINDOW,
                          () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);
            WorkflowStatus terminalAfterRollback = terminalWorkflowStatus(world.committedLog(), workflowId);
            int fulfilsAfterRollback = effects.count(workflowId, RollingDeployWorkflow.STEP_FULFILL);
            // The drift pause keeps the instance: it stays registered for recovery, non-terminal. Bounded window in
            // which an eviction would show.
            Polling.await(ABSENCE_WINDOW, () -> !world.engine().liveWorkflowIds().contains(workflowId));
            boolean liveAfterPause = world.engine().liveWorkflowIds().contains(workflowId);

            // 3. The ROLL-FORWARD: the bad v2 again, the body that MATCHES the polluted history. The documented drift
            // remedy: the next replay runs cleanly, so the restored instance consumes nothing new, runs fulfill once
            // and completes.
            world.crashAndRecoverWith(List.of(EngineInstance.rollingDeployWorkflowV2BadOnly(effects)));
            Polling.await(DEADLINE, () -> world.engine().liveWorkflowIds().contains(workflowId)
                    || terminalWorkflowStatus(world.committedLog(), workflowId) != null);
            boolean restored = world.engine().liveWorkflowIds().contains(workflowId)
                    || terminalWorkflowStatus(world.committedLog(), workflowId) != null;
            Polling.await(DEADLINE, () -> terminalWorkflowStatus(world.committedLog(), workflowId) != null);

            // Nothing re-drives or re-publishes after the terminal: a further restart must leave the record as is.
            world.crashAndRecoverWith(List.of(EngineInstance.rollingDeployWorkflowV2BadOnly(effects)));
            Polling.await(ABSENCE_WINDOW,
                          () -> workflowStatusRecords(world.committedLog(), workflowId, WorkflowStatus.COMPLETED) > 1
                                  || effects.count(workflowId, RollingDeployWorkflow.STEP_FULFILL) > 1);

            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new BadDeployOutcome(ranSilently, markersAfterBad, versionsAfterBad, terminalAfterRollback,
                                        fulfilsAfterRollback,
                                        hasStepRecord(world.committedLog(), workflowId,
                                                      RollingDeployWorkflow.STEP_AWAIT_APPROVAL, StepStatus.COMPLETED),
                                        liveAfterPause, restored,
                                        terminalWorkflowStatus(world.committedLog(), workflowId),
                                        effects.count(workflowId, RollingDeployWorkflow.STEP_FRAUD_CHECK),
                                        effects.count(workflowId, RollingDeployWorkflow.STEP_FULFILL),
                                        workflowStatusRecords(world.committedLog(), workflowId,
                                                              WorkflowStatus.COMPLETED),
                                        world.engine().liveWorkflowIds().contains(workflowId));
        }
    }

    /**
     * Waits until the instance is parked on its approval wait (STARTED recorded).
     */
    private static void awaitParked(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(DEADLINE, "instance " + workflowId + " to park on awaitApproval",
                            () -> hasStepRecord(world.committedLog(), workflowId,
                                                RollingDeployWorkflow.STEP_AWAIT_APPROVAL, StepStatus.STARTED));
    }

    /**
     * Waits until the RECOVERED instance has re-registered its wait (live + the rescheduled wait timer) — the F-16
     * discipline: a wake delivered before re-registration is silently lost.
     */
    private static void awaitReParked(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(DEADLINE, "instance " + workflowId + " to be LIVE in the recovered engine",
                            () -> world.engine().liveWorkflowIds().contains(workflowId));
        Polling.awaitOrFail(DEADLINE, "the recovered wait to be re-registered (timeout rescheduled)",
                            () -> world.scheduler().pendingTasks() > 0);
    }

    /**
     * The distinct {@code MessageType.version()} values the instance's committed events carry (insertion-ordered).
     */
        private static Set<String> distinctVersions(List<EventMessage> committedLog, String workflowId) {
        Set<String> versions = new LinkedHashSet<>();
        committedLog.stream()
                    .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                    .forEach(e -> versions.add(e.type().version()));
        return versions;
    }

    /**
     * Counts the {@link RollingDeployWorkflow#CHANGE_ID} migration markers this instance recorded.
     */
    private static long markerCount(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                           .filter(e -> RollingDeployWorkflow.CHANGE_ID.equals(
                                   MetadataUtils.getVersionChangeId(e.metadata()).orElse(null)))
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
}
