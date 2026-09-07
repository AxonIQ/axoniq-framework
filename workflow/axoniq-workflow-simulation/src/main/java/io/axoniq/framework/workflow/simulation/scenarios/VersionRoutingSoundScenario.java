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
import io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersionedOrderRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Deterministic scenario for INVARIANTS.md INV-11 ({@code VersionRoutingSound}): with two versions of one workflow
 * registered (same {@code workflowName}/start event, different {@code workflowVersion} — the multi-version registration
 * the {@code MultiVersionRoutingDeclarativeTest} example exercises), a fresh start routes to <strong>exactly one</strong>
 * definition at the <strong>highest</strong> registered version, and that version resolution is
 * <strong>deterministic across replay</strong>.
 * <p>
 * It drives {@link VersionedOrderWorkflow} registered at both {@link VersionedOrderWorkflow#VERSION_V1 1.0.0} and
 * {@link VersionedOrderWorkflow#VERSION_V2 1.0.1} (via {@link EngineInstance#versionedOrderWorkflow}). A fresh start must
 * pick the highest version (v2), run the v2 body (recording the v2-only {@code processV2} step, never the v1-only
 * {@code chargeV1}), and stamp every committed event with version {@code 1.0.1}.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the engine spawns at the highest registered version and the v2 body runs to COMPLETED
 *       — the committed log gets the workflow-status STARTED, the step records, and the terminal COMPLETED, all carrying
 *       version {@code 1.0.1};</li>
 *   <li>snapshot the instance's resolved version (the distinct {@code MessageType.version()} set) and the step names;
 *       {@link Invariants#assertVersionRoutingSound} must already hold (exactly one version, the highest);</li>
 *   <li>crash + recover (drives the real replay path), then assert the resolved version is byte-for-byte unchanged —
 *       replay resolved the SAME version for the instance (deterministic across replay), still exactly one, still the
 *       highest.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class VersionRoutingSoundScenario {

    private VersionRoutingSoundScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal      whether the instance reached a terminal (COMPLETED) workflow status.
     * @param resolvedVersionsBeforeCrash the distinct {@code MessageType.version()} values the instance's committed
     *                                    events carried at the terminal point. INV-11 requires this to be exactly
     *                                    {@code {1.0.1}} (one definition, the highest version).
     * @param resolvedVersionsAfterCrash  the same distinct-version set after a crash + replay. INV-11's
     *                                    deterministic-across-replay facet requires this to equal
     *                                    {@code resolvedVersionsBeforeCrash}.
     * @param ranV2OnlyStep        whether the v2-only {@code processV2} step was recorded (a fresh start ran the v2
     *                             body — the highest version).
     * @param ranV1OnlyStep        whether the v1-only {@code chargeV1} step was recorded; must be {@code false} (the v1
     *                             body must never run for a fresh start).
     */
    public record Outcome(boolean reachedTerminal, Set<String> resolvedVersionsBeforeCrash,
                          Set<String> resolvedVersionsAfterCrash, boolean ranV2OnlyStep,
                          boolean ranV1OnlyStep) {

    }

    /**
     * Runs the scenario against a fresh world registering both versions of {@link VersionedOrderWorkflow} and returns
     * what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registrations = EngineInstance.versionedOrderWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registrations)) {
            String workflowId = "vorder-" + orderId;

            // 1. Start the workflow: a fresh start spawns at the highest registered version (v2) and the v2 body runs to
            // COMPLETED on its own (execute-only).
            world.engine().publish(new VersionedOrderRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "versioned instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Snapshot the resolved version(s) + step names at the terminal point. INV-11 must already hold.
            Set<String> versionsBefore = resolvedVersions(world.committedLog(), workflowId);
            boolean ranV2 = hasStep(world.committedLog(), workflowId, VersionedOrderWorkflow.STEP_PROCESS_V2);
            boolean ranV1 = hasStep(world.committedLog(), workflowId, VersionedOrderWorkflow.STEP_CHARGE_V1);
            Invariants.assertVersionRoutingSound(world.committedLog(), "vorder-", VersionedOrderWorkflow.VERSION_V2,
                                                 Set.of(workflowId));

            // 3. IN-SCOPE INV-11 (deterministic across replay): a crash + replay must resolve the SAME version. Replay
            // re-runs the (already-recorded) body and emits nothing new; the resolved version set must be unchanged.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2),
                          () -> !resolvedVersions(world.committedLog(), workflowId).equals(versionsBefore));
            Set<String> versionsAfter = resolvedVersions(world.committedLog(), workflowId);
            Invariants.assertVersionRoutingSound(world.committedLog(), "vorder-", VersionedOrderWorkflow.VERSION_V2,
                                                 Set.of(workflowId));

            return new Outcome(true, versionsBefore, versionsAfter, ranV2, ranV1);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * Returns the distinct {@code MessageType.version()} values carried by the instance's committed events.
     */
        private static Set<String> resolvedVersions(List<EventMessage> committedLog, String workflowId) {
        Set<String> versions = new TreeSet<>();
        for (EventMessage event : committedLog) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                versions.add(event.type().version());
            }
        }
        return versions;
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId,
                                   String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).isPresent()
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }
}
