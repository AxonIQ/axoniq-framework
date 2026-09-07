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

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersioningEdgesRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersioningEdgesSignalEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Deterministic scenario for INVARIANTS.md INV-20 ({@code VersioningEdges}): the versioning edges INV-11
 * ({@code VersionRoutingSound}) and INV-12 ({@code MigrateVersionContract}) do not cover. It drives
 * {@link VersioningEdgesWorkflow} registered at <strong>three</strong> versions
 * ({@link VersioningEdgesWorkflow#VERSION_LOW 1.0.0}/{@link VersioningEdgesWorkflow#VERSION_MID 1.5.0}/
 * {@link VersioningEdgesWorkflow#VERSION_HIGH 2.0.0}) and exercises, end-to-end:
 * <ol>
 *   <li><strong>fresh-spawn + downgrade-rejected + multi-changeId:</strong> a fresh start spawns at the highest
 *       registered version (2.0.0), performs two forward migrations under distinct {@code changeId}s (recording 2.1.0
 *       then 2.2.0, monotonic), and attempts a downgrade migration that the engine rejects (never recorded — the body
 *       records an observable {@code downgradeRejected} step instead), then completes; a crash + replay re-reaches every
 *       migration as a no-op (marker counts + recorded versions unchanged);</li>
 *   <li><strong>deeper closest-sibling routing:</strong> a second instance is driven to the suspension point (recorded
 *       post-migration at 2.2.0) while still mid-flight, then crashed and recovered under a <em>reduced</em> registry
 *       whose highest registered version (2.0.0) was dropped (only 1.0.0 + 1.5.0 remain). The 4/5-pass lookup must route
 *       the instance recorded at 2.2.0 to the closest registered sibling {@code <= 2.2.0} — 1.5.0 (mid) — never 0 (no
 *       body &rarr; stranded), never 2; delivering its awaited signal post-recovery runs its final step and completes
 *       it, observably proving the routing landed on a runnable body.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class VersioningEdgesScenario {

    private static final String ID_PREFIX = "vedge-";

    private VersioningEdgesScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal              whether the fresh-spawn instance reached a terminal (COMPLETED) status.
     * @param spawnVersion                 the version the fresh-spawn instance's {@code STARTED} event carried. INV-20
     *                                     requires the highest registered version (2.0.0).
     * @param recordedMigrationVersions    the recorded migration versions (append order) of the fresh-spawn instance.
     *                                     INV-20 requires {@code [2.1.0, 2.2.0]} (two distinct changeIds, monotonic).
     * @param downgradeRecorded            whether ANY migration marker for the downgrade {@code changeId} was committed.
     *                                     INV-20 requires {@code false} (the downgrade was rejected, never recorded).
     * @param downgradeRejectedStepRan     whether the observable {@code downgradeRejected} step was recorded (the
     *                                     downgrade migration was genuinely attempted and rejected — non-vacuous).
     * @param markerCountBeforeCrash       total migration markers the fresh-spawn instance recorded before the crash.
     * @param markerCountAfterCrash        the same after a crash + replay. INV-20's replay-stability facet requires it to
     *                                     equal {@code markerCountBeforeCrash} (replay re-reaches each call as a no-op).
     * @param routedInstanceReachedTerminal whether the closest-sibling-routed instance completed after recovery under the
     *                                     reduced registry (the 4/5-pass lookup found a runnable body — never 0/never 2).
     * @param routedInstanceFinalized      whether the routed instance recorded its final step after recovery (the resumed
     *                                     body ran under the closest sibling).
     */
    public record Outcome(boolean reachedTerminal, String spawnVersion,
                          List<String> recordedMigrationVersions, boolean downgradeRecorded,
                          boolean downgradeRejectedStepRan, long markerCountBeforeCrash, long markerCountAfterCrash,
                          boolean routedInstanceReachedTerminal, boolean routedInstanceFinalized) {

    }

    /**
     * Runs the scenario (both flows) and returns what it observed.
     *
     * @param seed seed for the worlds' deterministic id source.
     * @return the observed outcome.
     */
        public static Outcome run(long seed) {
        FreshSpawn fresh = runFreshSpawn(seed, "A");
        ClosestSibling routed = runClosestSiblingRouting(seed, "B");
        return new Outcome(fresh.reachedTerminal, fresh.spawnVersion, fresh.recordedVersions, fresh.downgradeRecorded,
                           fresh.downgradeRejectedStepRan, fresh.markerBefore, fresh.markerAfter,
                           routed.reachedTerminal, routed.finalized);
    }

    private record FreshSpawn(boolean reachedTerminal, String spawnVersion, List<String> recordedVersions,
                              boolean downgradeRecorded, boolean downgradeRejectedStepRan, long markerBefore,
                              long markerAfter) {
    }

    /**
     * Flow 1: full 3-version registry. Fresh start &rarr; spawn at highest, two forward migrations, rejected downgrade,
     * complete; then crash + replay and confirm the migration record is stable (re-reached as a no-op).
     */
    private static FreshSpawn runFreshSpawn(long seed, String orderId) {
        var registrations = EngineInstance.versioningEdgesWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registrations)) {
            String workflowId = ID_PREFIX + orderId;

            // 1. Start; spawn at the highest registered version (2.0.0). The body migrates forward twice and attempts a
            // rejected downgrade, then suspends on awaitSignal. Re-deliver the signal a few times so a delivery lands
            // after the body has registered the wait association (at-least-once); the wait completes on the first match.
            world.engine().publish(new VersioningEdgesRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "vedge instance to record the downgradeRejected step",
                                () -> hasStep(world.committedLog(), workflowId,
                                              VersioningEdgesWorkflow.STEP_DOWNGRADE_REJECTED));
            for (int attempt = 0; attempt < 5 && !isTerminal(world.committedLog(), workflowId); attempt++) {
                world.engine().publish(new VersioningEdgesSignalEvent(orderId));
                Polling.await(Duration.ofSeconds(2), () -> isTerminal(world.committedLog(), workflowId));
            }
            Polling.awaitOrFail(Duration.ofSeconds(10), "vedge instance to reach a terminal status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Snapshot + assert INV-20 holds: spawn at highest, downgrade rejected (never recorded), markers monotonic.
            String spawnVersion = startedVersion(world.committedLog(), workflowId);
            List<String> recordedVersions = recordedMigrationVersions(world.committedLog(), workflowId);
            boolean downgradeRecorded = hasMigrationMarkerForChange(world.committedLog(), workflowId,
                                                                    VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE);
            boolean rejectedStepRan = hasStep(world.committedLog(), workflowId,
                                              VersioningEdgesWorkflow.STEP_DOWNGRADE_REJECTED);
            long markerBefore = totalMigrationMarkers(world.committedLog(), workflowId);
            Invariants.assertVersioningEdges(world.committedLog(), ID_PREFIX, VersioningEdgesWorkflow.VERSION_HIGH,
                                             VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE, true, Set.of(workflowId));

            // 3. Crash + replay: the migration record must be stable (each call re-reached as a no-op).
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2),
                          () -> totalMigrationMarkers(world.committedLog(), workflowId) != markerBefore);
            long markerAfter = totalMigrationMarkers(world.committedLog(), workflowId);
            Invariants.assertVersioningEdges(world.committedLog(), ID_PREFIX, VersioningEdgesWorkflow.VERSION_HIGH,
                                             VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE, true, Set.of(workflowId));

            return new FreshSpawn(isTerminal(world.committedLog(), workflowId), spawnVersion, recordedVersions,
                                  downgradeRecorded, rejectedStepRan, markerBefore, markerAfter);
        }
    }

    private record ClosestSibling(boolean reachedTerminal, boolean finalized) {
    }

    /**
     * Flow 2: deeper closest-sibling routing. Drive an instance to the suspension point (recorded post-migration at
     * 2.2.0) while still mid-flight, then crash and recover under a registry whose highest registered version (2.0.0)
     * was dropped. The 4/5-pass lookup must route the instance recorded at 2.2.0 to the closest registered sibling
     * {@code <= 2.2.0} — 1.5.0 (mid) — never 0/never 2; delivering its signal post-recovery completes it under that
     * routed body (the observable that the routing landed on a runnable body).
     */
    private static ClosestSibling runClosestSiblingRouting(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.versioningEdgesWorkflow(effects))) {
            String workflowId = ID_PREFIX + orderId;

            // 1. Drive to the mid-flight suspension point: reserveInventory + the two migrations + downgradeRejected
            // recorded, finalize NOT yet (the body is parked on awaitSignal, recorded post-migration at 2.2.0). Do NOT
            // deliver the signal yet — the instance must still be mid-flight at the crash so the resumed final step runs
            // under whatever the recovered registry routes it to.
            world.engine().publish(new VersioningEdgesRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "vedge routing instance to suspend at awaitSignal",
                                () -> hasStep(world.committedLog(), workflowId,
                                              VersioningEdgesWorkflow.STEP_DOWNGRADE_REJECTED)
                                        && !isTerminal(world.committedLog(), workflowId));

            // 2. Crash + recover under the REDUCED registry (highest version 2.0.0 dropped; only 1.0.0 + 1.5.0 remain).
            // The instance recorded (post-migration) at 2.2.0 routes via the closest registered sibling <= 2.2.0 = 1.5.0.
            world.crashAndRecoverWith(EngineInstance.versioningEdgesWorkflowReducedRegistry(effects));

            // 3. Deliver the awaited signal post-recovery; the resumed body (routed to the closest sibling) runs finalize
            // and completes. The instance completing under a registry that does NOT contain its exact recorded version
            // proves the 4/5-pass closest-sibling lookup found a runnable body (never 0, never 2). Re-deliver a few times
            // so a delivery lands after the recovered engine has replayed and re-parked the wait (the at-least-once /
            // fair-scheduling assumption); the wait completes on the first matching delivery.
            for (int attempt = 0; attempt < 5 && !isTerminal(world.committedLog(), workflowId); attempt++) {
                world.engine().publish(new VersioningEdgesSignalEvent(orderId));
                Polling.await(Duration.ofSeconds(2), () -> isTerminal(world.committedLog(), workflowId));
            }
            Polling.awaitOrFail(Duration.ofSeconds(5), "recovered vedge routing instance to resume + complete",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 4. INV-20 deeper-routing facet: the instance still resolves soundly (no recorded downgrade, markers stable,
            // no event stamped below the started version) after routing to the closest sibling. requireHighestForFresh
            // is FALSE here — under the reduced registry the highest REGISTERED version changed, but the instance keeps
            // its pinned recorded version; the routing landing on a runnable body is observed by the completion above.
            Invariants.assertVersioningEdges(world.committedLog(), ID_PREFIX, VersioningEdgesWorkflow.VERSION_HIGH,
                                             VersioningEdgesWorkflow.CHANGE_ID_DOWNGRADE, false, Set.of(workflowId));

            boolean finalized = hasStep(world.committedLog(), workflowId, VersioningEdgesWorkflow.STEP_FINALIZE);
            return new ClosestSibling(isTerminal(world.committedLog(), workflowId), finalized);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent()
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }

        private static String startedVersion(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                          .map(s -> s == io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus.STARTED)
                                          .orElse(false))
                .map(e -> e.type().version())
                .findFirst()
                .orElse("<none>");
    }

        private static List<String> recordedMigrationVersions(List<EventMessage> committedLog,
                                                          String workflowId) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .map(e -> MetadataUtils.getVersion(e.metadata()).orElse(null))
                .filter(v -> v != null)
                .toList();
    }

    private static boolean hasMigrationMarkerForChange(List<EventMessage> committedLog,
                                                       String workflowId, String changeId) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .anyMatch(e -> changeId.equals(MetadataUtils.getVersionChangeId(e.metadata()).orElse(null)));
    }

    private static long totalMigrationMarkers(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .count();
    }
}
