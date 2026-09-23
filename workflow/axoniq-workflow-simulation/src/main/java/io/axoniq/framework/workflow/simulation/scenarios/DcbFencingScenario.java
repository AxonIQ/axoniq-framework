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

import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.simulation.harness.LogCapture;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.test.fakes.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.fakes.MutableClock;
import io.axoniq.framework.workflow.runtime.test.fakes.SeededWorkflowIdGenerator;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.ControllableEventStorageEngine;
import io.axoniq.framework.workflow.simulation.harness.DurableTokenStore;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

/**
 * The {@link SplitBrainScenario} topology under DCB append conditions: the FENCED world.
 * <p>
 * Two {@link EngineInstance}s over one shared durable event store, each with its own non-durable per-process
 * processor token, both claim segment 0 and both route the same published start event — the F-1 split-brain race.
 * With {@code WorkflowAppendConditions} active, the shared store is the arbiter: the second writer's conditional
 * append is rejected ({@code AppendEventsTransactionRejectedException}), that execution is interrupted, and the
 * committed log stays single-writer clean. The former F-1 expected-gap observations (≥2 owners, ≥2 terminal records
 * for one step) must no longer be reachable.
 * <p>
 * The scenario reports what the fenced world must guarantee — at most one workflow-status STARTED record, at most one
 * terminal record per {@code (workflowId, stepName)}, at most one workflow-terminal record — plus the fault-landed
 * proof: how many append rejections {@code WorkflowAppendConditions} logged for this instance (observed via a
 * {@code LogCapture} on that class's logger). A run without an observed rejection proves nothing about fencing
 * (the race may not have fired) and must be treated as INCONCLUSIVE by the caller, not as a pass.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DcbFencingScenario {

    private static final String APPEND_CONDITIONS_LOGGER = "io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution";
    private static final Duration RACE_BUDGET = Duration.ofSeconds(12);

    private DcbFencingScenario() {
    }

    /**
     * Result of the scenario.
     *
     * @param startedRecords          committed records carrying workflow status {@code STARTED} for the instance
     *                                (fenced world: at most 1).
     * @param maxTerminalRecordsForAStep the highest number of terminal step records the shared durable log holds for
     *                                any single {@code (workflowId, stepName)} (fenced world: at most 1).
     * @param workflowTerminalRecords committed records carrying a terminal workflow status for the instance
     *                                (fenced world: at most 1; usually 0 here — the winner parks at
     *                                {@code awaitConfirmation}).
     * @param rejectionsObserved      rejection warnings {@code WorkflowAppendConditions} logged for this instance —
     *                                the fault-landed proof. 0 means the race did not provably fire: INCONCLUSIVE.
     * @param liveOwners              engines still holding the instance registered at the end (diagnostic only; a
     *                                rejected loser keeps its execution registered with its driver stopped, so 2 is
     *                                expected once a rejection fired).
     */
    public record Outcome(int startedRecords,
                          int maxTerminalRecordsForAStep,
                          int workflowTerminalRecords,
                          int rejectionsObserved,
                          int liveOwners) {

    }

    /**
     * Runs the two-engine race over a shared durable event store, with fencing expected to hold.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key for the order whose start event both engines see.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var eventStore = new ControllableEventStorageEngine();
        var effects = new CountingEffects();
        String workflowId = "order-" + orderId;

        // Fault-landed observation: capture the rejection warnings WorkflowAppendConditions logs for this instance.
        var rejections = LogCapture.attach(APPEND_CONDITIONS_LOGGER);
        try {
            var safePointA = new DurableTokenStore();
            var safePointB = new DurableTokenStore();
            var historyA = new InMemoryWorkflowHistoryRepository();
            var historyB = new InMemoryWorkflowHistoryRepository();
            var schedulerA = new ManualWorkflowScheduler();
            var schedulerB = new ManualWorkflowScheduler();
            var clockA = new MutableClock(Instant.EPOCH);
            var clockB = new MutableClock(Instant.EPOCH);

            try (var engineA = new EngineInstance(eventStore, safePointA, historyA, schedulerA, clockA,
                                                  new SeededWorkflowIdGenerator("a", seed), effects);
                 var engineB = new EngineInstance(eventStore, safePointB, historyB, schedulerB, clockB,
                                                  new SeededWorkflowIdGenerator("b", seed), effects)) {

                // Both nodes see the start event; both processors independently claim segment 0 and route it, so both
                // spawn the instance and both try to append its records to the SHARED store — the F-1 race.
                engineA.publish(new OrderPlacedEvent(orderId));
                engineB.publish(new OrderPlacedEvent(orderId));

                // Fenced world: the loser's conditional append is rejected and its execution interrupted, while the
                // winner drives the first execute step (reserveInventory) to COMPLETED and parks at awaitConfirmation.
                // Wait (bounded) for both signals; if the rejection never shows, the caller marks the seed INCONCLUSIVE.
                String firstStep = OrderWorkflow.STEP_RESERVE_INVENTORY;
                Polling.await(RACE_BUDGET,
                              () -> rejectionsFor(rejections, workflowId) >= 1
                                      && terminalRecords(eventStore, workflowId, firstStep) >= 1);

                int liveOwners = 0;
                if (engineA.liveWorkflowIds().contains(workflowId)) {
                    liveOwners++;
                }
                if (engineB.liveWorkflowIds().contains(workflowId)) {
                    liveOwners++;
                }
                return new Outcome(startedRecords(eventStore, workflowId),
                                   maxTerminalRecordsForAnyStep(eventStore, workflowId),
                                   workflowTerminalRecords(eventStore, workflowId),
                                   rejectionsFor(rejections, workflowId),
                                   liveOwners);
            }
        } finally {
            rejections.close();
        }
    }

    /**
     * Counts the rejection warnings logged for the given instance so far.
     */
    private static int rejectionsFor(LogCapture rejections, String workflowId) {
        // Snapshot to avoid iterating the live list while the engines append to it.
        int count = 0;
        for (String message : rejections.messages()) {
            if (message != null && message.contains("was rejected") && message.contains("'" + workflowId + "'")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts committed records carrying workflow status STARTED for the instance.
     */
    private static int startedRecords(ControllableEventStorageEngine eventStore, String workflowId) {
        int count = 0;
        for (var event : eventStore.committedWorkflowLog()) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                    && MetadataUtils.getWorkflowStatus(event.metadata())
                                    .filter(s -> s == io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus.STARTED)
                                    .isPresent()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts committed records carrying a terminal workflow status for the instance.
     */
    private static int workflowTerminalRecords(ControllableEventStorageEngine eventStore,
                                               String workflowId) {
        int count = 0;
        for (var event : eventStore.committedWorkflowLog()) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                    && MetadataUtils.getWorkflowStatus(event.metadata())
                                    .filter(io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus::isTerminal)
                                    .isPresent()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts terminal step records for one {@code (workflowId, stepName)} in the shared durable committed log.
     */
    private static long terminalRecords(ControllableEventStorageEngine eventStore,
                                        String workflowId, String stepName) {
        return eventStore.committedWorkflowLog().stream()
                .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(StepStatus::isTerminal).orElse(false)
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())))
                .count();
    }

    /**
     * Returns the highest terminal-record count over all step names for the given instance in the shared durable log.
     */
    private static int maxTerminalRecordsForAnyStep(ControllableEventStorageEngine eventStore,
                                                    String workflowId) {
        var terminalCountsByStep = new HashMap<String, Integer>();
        for (var event : eventStore.committedWorkflowLog()) {
            if (!workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                continue;
            }
            var status = MetadataUtils.getStepStatus(event.metadata());
            if (status.isEmpty() || !status.get().isTerminal()) {
                continue;
            }
            terminalCountsByStep.merge(MetadataUtils.getStepName(event.metadata()), 1, Integer::sum);
        }
        return terminalCountsByStep.values().stream().max(Integer::compareTo).orElse(0);
    }
}
