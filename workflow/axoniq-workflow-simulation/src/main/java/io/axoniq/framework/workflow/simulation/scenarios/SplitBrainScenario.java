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

/**
 * Hand-authored scenario that reproduces the F-1 split-brain finding (INVARIANTS.md INV-1, {@code AtMostOneOwner})
 * and its F-1 <em>consequence</em> on recording (INVARIANTS.md INV-2, {@code AtMostOnceRecording}).
 * <p>
 * The engine's processor {@code TokenStore} is hardcoded in-memory and per-process
 * ({@code AllEventEventHandlingComponent.java:70}), so it provides no cross-node single-writer lease. This scenario
 * stands up <strong>two</strong> {@link EngineInstance}s over one shared durable event store, each with its own
 * (separate, non-durable) processor token — modelling two nodes. Both claim segment 0 and both route a published
 * start event, so the same instance is driven by two owners at once: split-brain.
 * <p>
 * This is the implementation-level match of two TLA+ counterexamples that share the same non-durable-lease setup:
 * <ul>
 *   <li>{@code MC_owner.cfg} ({@code AtMostOneOwner} VIOLATED, F-1): both processes hold segment 0 — observed here as
 *       two engines that both routed the start event;</li>
 *   <li>{@code MC_record.cfg} ({@code AtMostOnceRecording} VIOLATED, F-1 consequence): with no append-condition on the
 *       {@code COMPLETED} write ({@code ExecuteDelegate.java:163…} FIXMEs), each owner drives the first {@code execute}
 *       step ({@code reserveInventory}) to {@code COMPLETED} from its own {@code EventSourcedWorkflowState} and both
 *       append a terminal record to the shared durable log — two {@code COMPLETED} records for one
 *       {@code (workflowId, stepName)}, exactly the TLA+ tail {@code log = ⟨STARTED, COMPLETED, COMPLETED⟩}.</li>
 * </ul>
 * The harness records both as documented expected violations (it expects ≥2 owners and ≥2 terminal records) rather
 * than build breaks, so the per-PR smoke run stays green; a durable lease / optimistic append-condition that closes
 * F-1 would make these expectations fail, the intended signal to re-evaluate.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class SplitBrainScenario {

    private SplitBrainScenario() {
    }

    /**
     * Result of the scenario.
     *
     * @param ownersThatRoutedStart      how many of the two engines created the instance for the published start event
     *                                   (2 = split-brain: both own segment 0; the {@code MC_owner.cfg} /
     *                                   {@code AtMostOneOwner} observation).
     * @param maxTerminalRecordsForAStep the highest number of terminal step records the shared durable log holds for
     *                                   any single {@code (workflowId, stepName)} (2 = the duplicate-recording
     *                                   consequence; the {@code MC_record.cfg} / {@code AtMostOnceRecording}
     *                                   observation).
     */
    public record Outcome(int ownersThatRoutedStart, int maxTerminalRecordsForAStep) {

    }

    /**
     * Runs the two-engine split-brain scenario over a shared durable event store.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key for the order whose start event both engines see.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var eventStore = new ControllableEventStorageEngine();
        var effects = new CountingEffects();
        String workflowId = "order-" + orderId;

        // Two independent "nodes": shared durable event store, but each gets its own safe-point store, history
        // read-model, virtual-time seams, and (inside EngineInstance) its own in-memory processor token.
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

            // Publish the start event through both nodes' sinks (the same durable store backs both); each node's
            // processor independently claims segment 0 and routes it, because the per-process token forbids nothing.
            engineA.publish(new OrderPlacedEvent(orderId));
            engineB.publish(new OrderPlacedEvent(orderId));

            // Give both processors time to create the instance and drive the first execute step to COMPLETED. The
            // first step (reserveInventory) is a pure execute with no wait, so each owner runs it and appends its own
            // STARTED+COMPLETED to the SHARED store before the body parks at awaitConfirmation. With no append-condition
            // (ExecuteDelegate FIXMEs), both COMPLETED records land: the MC_record duplicate-recording consequence.
            String firstStep = OrderWorkflow.STEP_RESERVE_INVENTORY;
            Polling.await(Duration.ofSeconds(10),
                          () -> engineA.liveWorkflowIds().contains(workflowId)
                                  && engineB.liveWorkflowIds().contains(workflowId)
                                  && terminalRecords(eventStore, workflowId, firstStep) >= 2);

            int owners = 0;
            if (engineA.liveWorkflowIds().contains(workflowId)) {
                owners++;
            }
            if (engineB.liveWorkflowIds().contains(workflowId)) {
                owners++;
            }
            int maxTerminalRecords = maxTerminalRecordsForAnyStep(eventStore, workflowId);
            return new Outcome(owners, maxTerminalRecords);
        }
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
        var terminalCountsByStep = new java.util.HashMap<String, Integer>();
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
