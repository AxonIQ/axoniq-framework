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
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Pins the liveness the fence buys safety with: "Two interleaved writers can both get rejected and both stop; the
 * instance stays durable and parks until the next claim restores it. Safety over liveness." (ADR-015, Consequences).
 * <p>
 * One foreign write lands for a live instance, once. Its owner is rejected on its next append, interrupted, and
 * removed from the engine. Nothing else on this node is going to touch that instance: no claim moves, no timer wakes
 * it, and the confirmation it was going to need is delivered to nobody. The scenario reports how far simulated time
 * runs with the instance still non-terminal, then restores the world the way the next claim would and reports that it
 * then finishes — the park is a park, not a corruption.
 * <p>
 * Landing evidence: the store reports the instance the fence wrote for, and the rejection is what stopped the owner.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedWriterParkScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration PARK_WINDOW = Duration.ofSeconds(3);
    private static final Duration SIMULATED_WAIT = Duration.ofHours(24);

    private FencedWriterParkScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param fencedInstance     the instance the fence wrote for; empty means the fence never fired.
     * @param terminalWhileParked whether the instance reached a terminal record without any claim moving.
     * @param simulatedParkTime  simulated time that passed with the instance parked.
     * @param terminalAfterClaim whether the next claim drove the instance to a terminal record.
     * @param workflowTerminalRecords terminal workflow records the instance ended with (must never exceed 1).
     */
    public record Outcome(String fencedInstance,
                          boolean terminalWhileParked,
                          Duration simulatedParkTime,
                          boolean terminalAfterClaim,
                          int workflowTerminalRecords) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the order that gets fenced.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "order-" + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.orderWorkflow(effects))) {
            world.engine().publish(new OrderPlacedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the instance to record its first step",
                                () -> stepRecords(world.committedLog(), workflowId,
                                                  OrderWorkflow.STEP_RESERVE_INVENTORY, StepStatus.COMPLETED) >= 1);

            // One late write from a writer that does not know it lost the instance.
            world.eventStore().armForeignWriteBeforeCommitOf(OrderWorkflow.STEP_CHARGE_PAYMENT, StepStatus.COMPLETED);
            Polling.awaitOrFail(DEADLINE, "the fence to consume the step's commit",
                                () -> !world.eventStore().isFenceArmed());
            // Read the landing evidence off the store that fired: a claim later replaces it with a fresh one.
            var fencedInstance = world.eventStore().fencedInstance().orElse("");

            // Nothing claims the instance now. Give it everything a healthy instance would need to finish.
            var parkStart = world.clock().instant();
            world.engine().publish(new PaymentConfirmedEvent(orderId));
            world.advanceTime(SIMULATED_WAIT);
            boolean terminalWhileParked = Polling.await(PARK_WINDOW,
                                                        () -> isTerminal(world.committedLog(), workflowId));
            var parkedFor = Duration.between(parkStart, world.clock().instant());

            // The next claim.
            world.crashAndRecover();
            world.engine().publish(new PaymentConfirmedEvent(orderId));
            boolean terminalAfterClaim = Polling.await(DEADLINE, () -> {
                world.advanceTime(Duration.ofSeconds(30));
                return isTerminal(world.committedLog(), workflowId);
            });

            return new Outcome(fencedInstance,
                               terminalWhileParked,
                               parkedFor,
                               terminalAfterClaim,
                               terminalRecords(world.committedLog(), workflowId));
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return terminalRecords(committedLog, workflowId) >= 1;
    }

    private static int terminalRecords(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                         && MetadataUtils.getWorkflowStatus(event.metadata())
                                                         .filter(WorkflowStatus::isTerminal).isPresent())
                                 .count();
    }

    private static int stepRecords(List<EventMessage> committedLog, String workflowId,
                                   String stepName, StepStatus status) {
        return (int) committedLog.stream()
                                 .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                         && stepName.equals(MetadataUtils.getStepName(event.metadata()))
                                         && MetadataUtils.getStepStatus(event.metadata())
                                                         .filter(s -> s == status).isPresent())
                                 .count();
    }
}
