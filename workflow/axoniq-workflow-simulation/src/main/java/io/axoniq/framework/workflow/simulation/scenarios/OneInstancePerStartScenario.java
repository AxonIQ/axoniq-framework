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

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.StartOnlyRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Deterministic scenario for INVARIANTS.md INV-10 ({@code OneInstancePerStart}): a single workflow start for a given
 * business key yields exactly one live instance, and a duplicate/redelivered start event delivered while the instance
 * is LIVE does not create a second concurrent instance (the engine dedups it via its in-memory spawn-dedup repository).
 * <p>
 * It is the live-dedup twin of {@code TerminalIsFinalScenario}'s F-3 probe. Where F-3 redelivers a start <em>after</em>
 * termination (and the terminated key re-spawns — the documented after-terminal exception INV-10 tolerates), this
 * scenario redelivers the start while the instance is still LIVE, exercising the dedup path
 * ({@code WorkflowSpawnRouting.resolveWorkflowIdForNewSpawn} returns {@code null} for a live duplicate at the same
 * version, logging "already running … treated as a duplicate"). It drives {@link io.axoniq.framework.workflow.simulation.workflow.StartOnlyWorkflow},
 * which runs one recorded step ({@code reserveInventory}) and then parks on a never-arriving {@code waitForEvent}, so
 * the instance stays non-terminal while the duplicate start arrives.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs {@code reserveInventory} and then suspends on its wait — the committed
 *       log gets the workflow-status STARTED + the step's STARTED/COMPLETED, and the instance stays LIVE;</li>
 *   <li>snapshot the workflow-status STARTED count and the live-instance count;</li>
 *   <li>redeliver the <em>same</em> start event (at-least-once delivery / the duplicate fault) while the instance is
 *       still live: a correct engine dedups it — <strong>no</strong> second instance and <strong>no</strong> second
 *       workflow-status STARTED appears;</li>
 *   <li>assert {@link Invariants#assertOneInstancePerStart} holds and the STARTED count + live-instance count are
 *       unchanged (exactly one live instance for the id).</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class OneInstancePerStartScenario {

    private OneInstancePerStartScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param instanceLive                 whether the single instance reached a live (non-terminal) state with its
     *                                     {@code reserveInventory} step recorded (so the dedup is exercised on a
     *                                     genuinely-live instance).
     * @param startedCountAfterFirstStart  number of committed workflow-status {@code STARTED} events for the instance
     *                                     after the first start. Expected to be exactly 1.
     * @param startedCountAfterDuplicate   number of committed workflow-status {@code STARTED} events for the instance
     *                                     after redelivering the start while live. INV-10 requires this to be unchanged
     *                                     from {@code startedCountAfterFirstStart} (still 1) — the duplicate start was
     *                                     deduped, not spawned as a second concurrent instance.
     * @param liveInstancesAfterDuplicate  number of LIVE engine executions whose id is this business key after the
     *                                     duplicate start. INV-10 requires this to be exactly 1.
     */
    public record Outcome(boolean instanceLive, int startedCountAfterFirstStart, int startedCountAfterDuplicate,
                          int liveInstancesAfterDuplicate) {

    }

    /**
     * Runs the scenario against a fresh world (driving {@link io.axoniq.framework.workflow.simulation.workflow.StartOnlyWorkflow})
     * and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.startOnlyWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "start-" + orderId;

            // 1. Start the workflow: reserveInventory runs, then the body parks on its never-arriving wait — the
            // instance is LIVE (non-terminal) with a single workflow-status STARTED recorded.
            world.engine().publish(new StartOnlyRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "instance to start and park on its wait (live, non-terminal)",
                                () -> startedCount(world.committedLog(), workflowId) >= 1
                                        && world.engine().liveWorkflowIds().contains(workflowId));

            // 2. Snapshot at the live point. INV-10 must already hold (exactly one STARTED).
            int startedAfterFirst = startedCount(world.committedLog(), workflowId);
            Invariants.assertOneInstancePerStart(world.committedLog());

            // 3. Redeliver the SAME start event while the instance is still LIVE (the duplicate/redeliver case). A
            // correct engine consults its in-memory spawn-dedup repository, finds the live instance and rejects the
            // spawn — no second instance, no second workflow-status STARTED. Give the engine a bounded window in which a
            // buggy dedup could (incorrectly) append a second STARTED.
            world.engine().publish(new StartOnlyRequestedEvent(orderId));
            Polling.await(Duration.ofSeconds(2),
                          () -> startedCount(world.committedLog(), workflowId) > startedAfterFirst);

            int startedAfterDuplicate = startedCount(world.committedLog(), workflowId);
            int liveInstances = (int) world.engine().liveWorkflowIds().stream()
                                           .filter(workflowId::equals)
                                           .count();
            // 4. INV-10 holds: the duplicate live start did not open a second concurrent lifecycle.
            Invariants.assertOneInstancePerStart(world.committedLog());

            boolean live = world.engine().liveWorkflowIds().contains(workflowId);
            return new Outcome(live, startedAfterFirst, startedAfterDuplicate, liveInstances);
        }
    }

    /**
     * Counts committed workflow-status {@code STARTED} events for a {@code workflowId} (one per spawned lifecycle).
     */
    private static int startedCount(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                          .map(s -> s == WorkflowStatus.STARTED).orElse(false))
                .count();
    }
}
