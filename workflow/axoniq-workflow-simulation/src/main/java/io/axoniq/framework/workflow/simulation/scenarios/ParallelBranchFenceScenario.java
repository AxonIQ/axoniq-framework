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
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

/**
 * Fences one parallel branch of an instance whose siblings are running at the same time.
 * <p>
 * Appends of one instance are chained on a single marker, so parallel steps commit one after another. That chain is
 * what keeps siblings from conflicting with each other; this asks what it does when a foreign writer breaks it in the
 * middle. One branch's {@code COMPLETED} commit is beaten to the store, the whole execution is interrupted by that
 * rejection, and the siblings that were still running lose their driver with it.
 * <p>
 * The oracle is the fencing contract on the durable log, in both halves of the run. While fenced: no branch may hold
 * two terminal records and the instance no second {@code STARTED} — a sibling must not race past the fence and record
 * an outcome anyway. After the next claim: the instance still reaches exactly one terminal record.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ParallelBranchFenceScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final Duration SETTLE_WINDOW = Duration.ofSeconds(2);

    private ParallelBranchFenceScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param fencedInstance             the instance the fence wrote for; empty means the fence never fired.
     * @param maxStartedRecords          highest number of {@code STARTED} workflow records for the instance.
     * @param maxTerminalRecordsForAStep highest number of terminal step records for any one step of the instance,
     *                                   measured while the execution is fenced.
     * @param terminalRecordsWhileFenced terminal workflow records while fenced.
     * @param terminalAfterClaim         whether the next claim drove the instance to a terminal record.
     * @param maxTerminalRecordsAtEnd    highest number of terminal step records for any one step at the end.
     * @param terminalRecordsAtEnd       terminal workflow records at the end.
     */
    public record Outcome(String fencedInstance,
                          int maxStartedRecords,
                          int maxTerminalRecordsForAStep,
                          int terminalRecordsWhileFenced,
                          boolean terminalAfterClaim,
                          int maxTerminalRecordsAtEnd,
                          int terminalRecordsAtEnd) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the combinator instance whose branch is fenced.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "comb-" + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.combinatorWorkflow(effects))) {
            world.eventStore().armForeignWriteBeforeCommitOf(CombinatorWorkflow.BRANCH_A, StepStatus.COMPLETED);
            world.engine().publish(new CombinatorRequestedEvent(orderId));

            Polling.awaitOrFail(DEADLINE, "the fence to consume a branch's commit",
                                () -> !world.eventStore().isFenceArmed());
            var fencedInstance = world.eventStore().fencedInstance().orElse("");
            // Let anything the siblings were about to record land before reading the log.
            Polling.await(SETTLE_WINDOW, () -> false);

            var whileFenced = world.committedLog();
            int startedWhileFenced = maxWorkflowRecords(whileFenced, workflowId, false);
            int maxStepWhileFenced = maxTerminalStepRecords(whileFenced, workflowId);
            int terminalWhileFenced = maxWorkflowRecords(whileFenced, workflowId, true);

            world.crashAndRecover();
            boolean terminalAfterClaim = Polling.await(DEADLINE, () -> {
                world.advanceTime(Duration.ofSeconds(5));
                return maxWorkflowRecords(world.committedLog(), workflowId, true) >= 1;
            });

            return new Outcome(fencedInstance,
                               Math.max(startedWhileFenced, maxWorkflowRecords(world.committedLog(), workflowId, false)),
                               maxStepWhileFenced,
                               terminalWhileFenced,
                               terminalAfterClaim,
                               maxTerminalStepRecords(world.committedLog(), workflowId),
                               maxWorkflowRecords(world.committedLog(), workflowId, true));
        }
    }

    private static int maxWorkflowRecords(List<EventMessage> committedLog, String workflowId,
                                          boolean terminal) {
        return (int) committedLog.stream()
                                 .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata())))
                                 .filter(event -> MetadataUtils.getWorkflowStatus(event.metadata())
                                                               .filter(status -> terminal
                                                                       ? status.isTerminal()
                                                                       : status == WorkflowStatus.STARTED)
                                                               .isPresent())
                                 .count();
    }

    private static int maxTerminalStepRecords(List<EventMessage> committedLog, String workflowId) {
        var counts = new HashMap<String, Integer>();
        for (EventMessage event : committedLog) {
            if (!workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                continue;
            }
            var status = MetadataUtils.getStepStatus(event.metadata());
            if (status.isEmpty() || !status.get().isTerminal()) {
                continue;
            }
            counts.merge(MetadataUtils.getStepName(event.metadata()), 1, Integer::sum);
        }
        return counts.values().stream().max(Integer::compareTo).orElse(0);
    }
}
