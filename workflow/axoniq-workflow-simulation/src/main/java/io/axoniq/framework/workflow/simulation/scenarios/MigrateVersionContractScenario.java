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
import io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.MigrateRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Deterministic scenario for INVARIANTS.md INV-12 ({@code MigrateVersionContract}): a workflow whose body calls
 * {@code ctx.migrateVersion(changeId, v)} records the migration version <strong>once</strong>, monotonic
 * non-decreasing (first-writer-wins, never downgrades), and a crash + replay resolves the <strong>same</strong>
 * recorded version unchanged (replay does not re-apply or change it).
 * <p>
 * It drives {@link MigratingOrderWorkflow} (registered as a single definition pinned to
 * {@link MigratingOrderWorkflow#INITIAL_VERSION 1.0.0}, via {@link EngineInstance#migratingOrderWorkflow}). A fresh
 * start runs the body, which migrates forward to {@link MigratingOrderWorkflow#MIGRATED_VERSION 1.0.1} via the
 * {@link MigratingOrderWorkflow#CHANGE_ID payment-redesign} change and takes the migrated branch.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs to COMPLETED, recording exactly one migration marker (a {@code COMPLETED}
 *       step event carrying the {@code versionChangeId} + {@code version} metadata keys) for {@code payment-redesign}
 *       with recorded version {@code 1.0.1};</li>
 *   <li>snapshot the recorded migration version and marker count; {@link Invariants#assertMigrateVersionContract} must
 *       already hold (recorded at most once, monotonic);</li>
 *   <li>crash + recover (drives the real replay path), then assert the recorded version and marker count are unchanged —
 *       replay re-reached the {@code migrateVersion} call as a no-op (it did not re-apply or change the recorded
 *       version), still exactly one marker, still {@code 1.0.1}.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class MigrateVersionContractScenario {

    private MigrateVersionContractScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal           whether the instance reached a terminal (COMPLETED) workflow status.
     * @param markerCountBeforeCrash    how many migration markers for {@link MigratingOrderWorkflow#CHANGE_ID} the
     *                                  instance recorded at the terminal point. INV-12 requires exactly one.
     * @param recordedVersionBeforeCrash the recorded migration version at the terminal point (the {@code version}
     *                                  metadata of the marker). INV-12 requires {@link MigratingOrderWorkflow#MIGRATED_VERSION}.
     * @param markerCountAfterCrash     the marker count after a crash + replay. INV-12's replay-stability facet requires
     *                                  it to equal {@code markerCountBeforeCrash} (replay does not re-apply the marker).
     * @param recordedVersionAfterCrash the recorded migration version after a crash + replay. INV-12's replay-stability
     *                                  facet requires it to equal {@code recordedVersionBeforeCrash}.
     * @param tookMigratedBranch        whether the migrated {@code processV2} step was recorded (the migration returned
     *                                  {@code true} for a fresh start).
     * @param tookLegacyBranch          whether the legacy {@code chargeV1} step was recorded; must be {@code false} (a
     *                                  fresh start migrates forward).
     */
    public record Outcome(boolean reachedTerminal, long markerCountBeforeCrash,
                          String recordedVersionBeforeCrash, long markerCountAfterCrash,
                          String recordedVersionAfterCrash, boolean tookMigratedBranch,
                          boolean tookLegacyBranch) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link MigratingOrderWorkflow} and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.migratingOrderWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "vmig-" + orderId;

            // 1. Start the workflow: a fresh start runs the body, migrates forward to MIGRATED_VERSION, and completes on
            // its own (execute-only).
            world.engine().publish(new MigrateRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "migrating instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Snapshot the recorded migration version + marker count at the terminal point. INV-12 must already hold.
            long markerBefore = markerCount(world.committedLog(), workflowId);
            String versionBefore = recordedVersion(world.committedLog(), workflowId).orElse("<none>");
            boolean migrated = hasStep(world.committedLog(), workflowId, MigratingOrderWorkflow.STEP_PROCESS_V2);
            boolean legacy = hasStep(world.committedLog(), workflowId, MigratingOrderWorkflow.STEP_CHARGE_V1);
            Invariants.assertMigrateVersionContract(world.committedLog(), "vmig-");

            // 3. IN-SCOPE INV-12 (replay-stability): a crash + replay must resolve the SAME recorded version and NOT
            // re-apply the marker. Replay re-runs the (already-recorded) body; the migrateVersion call must be a no-op.
            world.crashAndRecover();
            // Give replay a moment to (potentially) misbehave; the marker count / recorded version must stay unchanged.
            Polling.await(Duration.ofSeconds(2),
                          () -> markerCount(world.committedLog(), workflowId) != markerBefore);
            long markerAfter = markerCount(world.committedLog(), workflowId);
            String versionAfter = recordedVersion(world.committedLog(), workflowId).orElse("<none>");
            Invariants.assertMigrateVersionContract(world.committedLog(), "vmig-");

            return new Outcome(true, markerBefore, versionBefore, markerAfter, versionAfter, migrated, legacy);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * Counts the migration markers for {@link MigratingOrderWorkflow#CHANGE_ID} this instance recorded.
     */
    private static long markerCount(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .filter(e -> MigratingOrderWorkflow.CHANGE_ID.equals(
                        MetadataUtils.getVersionChangeId(e.metadata()).orElse(null)))
                .count();
    }

    /**
     * Returns the recorded migration version (the {@code version} metadata of the first {@link MigratingOrderWorkflow#CHANGE_ID}
     * marker) for this instance, or empty if it has not migrated yet.
     */
        private static Optional<String> recordedVersion(List<EventMessage> committedLog,
                                                    String workflowId) {
        return committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.isVersionMigrationStep(e.metadata()))
                .filter(e -> MigratingOrderWorkflow.CHANGE_ID.equals(
                        MetadataUtils.getVersionChangeId(e.metadata()).orElse(null)))
                .map(e -> MetadataUtils.getVersion(e.metadata()).orElse(null))
                .filter(v -> v != null)
                .findFirst();
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent()
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }
}
