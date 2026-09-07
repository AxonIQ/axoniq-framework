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
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.MigrateRequestedEvent;

import java.time.Duration;

/**
 * A foreign write lands right after an in-body version migration recorded its marker, before the migrated branch's
 * next append.
 * <p>
 * The migration marker is the record that decides which code every later incarnation of the instance runs. A stale
 * node that gets past it and keeps appending would drive the migrated branch on behalf of a node that never chose it,
 * and a second marker would make the routing itself ambiguous.
 * <p>
 * Oracle: nothing more is accepted for the instance after the foreign write, and it holds exactly one migration
 * marker, with the rejection warning as the proof the migrated branch really did try to append.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedVersionMigrationScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);

    private FencedVersionMigrationScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param recordsBeforeFence records the instance held when the foreign write landed.
     * @param recordsAfterFence  records it holds after the migrated branch ran on the fenced node.
     * @param migrationMarkers   committed migration marker records (must be exactly 1).
     * @param terminalRecords    terminal workflow records (must be 0).
     * @param rejections         rejection warnings for the instance (must be at least 1).
     */
    public record Outcome(int recordsBeforeFence, int recordsAfterFence, int migrationMarkers,
                          int terminalRecords, int rejections) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the migrating instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "vmig-" + orderId;
        var marker = MigratingOrderWorkflow.CHANGE_ID;
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed, EngineInstance.migratingOrderWorkflow(effects))) {
            world.engine().publish(new MigrateRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the migration marker to commit",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId, marker,
                                                               StepStatus.COMPLETED) >= 1);

            int before = FenceOracles.records(world.committedLog(), workflowId);
            world.eventStore().appendForeign(workflowId);

            Polling.await(REJECTION_WINDOW, () -> FenceOracles.rejections(appender, workflowId) >= 1);
            return new Outcome(before,
                               FenceOracles.records(world.committedLog(), workflowId),
                               FenceOracles.stepRecords(world.committedLog(), workflowId, marker,
                                                        StepStatus.COMPLETED),
                               FenceOracles.terminalRecords(world.committedLog(), workflowId),
                               FenceOracles.rejections(appender, workflowId));
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
