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

import io.axoniq.framework.workflow.simulation.harness.LogCapture;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Fences a step's own {@code STARTED} append and checks that the step's action never runs.
 * <p>
 * "Run a step's action only once the store accepted its STARTED" is the guarantee this covers. A run waits for the
 * step to turn {@code STARTED} before invoking the action, and that state change carries no writer identity — it can
 * be the {@code STARTED} another execution of the same instance recorded, arriving over this execution's own stream.
 * The store accepting this execution's own append is the only proof the step is this execution's.
 * <p>
 * The fence is deterministic: the store writes a foreign event for the instance in the window between the condition
 * check that creates the append transaction and the one that commits it, so the run's own {@code STARTED} is rejected
 * after it was already accepted once ({@code ControllableEventStorageEngine#armForeignWriteBeforeCommitOf}).
 * <p>
 * The oracle has both directions. While the run is fenced: the action has not run, no {@code STARTED} record for the
 * step is in the durable log, and the fenced execution published nothing terminal. After the next claim restores the
 * instance: the action has run exactly once, so the fence costs a retry of the step and not the step itself.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedStepStartScenario {

    private static final String REJECTION_LOGGER = "io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution";
    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);

    private FencedStepStartScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param fencedInstance          the instance the fence wrote for — empty means the fence never fired and the run
     *                                proves nothing.
     * @param rejectionsObserved      {@code "was rejected"} warnings logged for the instance while fenced.
     * @param effectRunsWhileFenced   times the fenced step's action ran before the next claim (must be 0).
     * @param startedRecordsWhileFenced durable {@code STARTED} records for the fenced step while fenced (must be 0).
     * @param terminalRecordsWhileFenced durable terminal workflow records while fenced (must be 0).
     * @param effectRunsAfterRestore  times the step's action ran once the next claim restored the instance.
     * @param completedRecordsAfterRestore durable {@code COMPLETED} records for the step after the restore.
     */
    public record Outcome(String fencedInstance,
                          int rejectionsObserved,
                          int effectRunsWhileFenced,
                          int startedRecordsWhileFenced,
                          int terminalRecordsWhileFenced,
                          int effectRunsAfterRestore,
                          int completedRecordsAfterRestore) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the order whose first step is fenced.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "order-" + orderId;
        var step = OrderWorkflow.STEP_RESERVE_INVENTORY;

        var rejections = LogCapture.attach(REJECTION_LOGGER);
        try (var world = new SimulationWorld(seed, EngineInstance.orderWorkflow(effects))) {
            world.eventStore().armForeignWriteBeforeCommitOf(step, StepStatus.STARTED);
            world.engine().publish(new OrderPlacedEvent(orderId));

            Polling.awaitOrFail(DEADLINE, "the fence to consume the step's STARTED commit",
                                () -> !world.eventStore().isFenceArmed());
            Polling.awaitOrFail(DEADLINE, "the fenced execution to log its rejection",
                                () -> rejectionsFor(rejections, workflowId) >= 1);
            // Give a late action invocation a window to show up rather than asserting on an empty instant.
            Polling.await(ABSENCE_WINDOW, () -> effects.count(workflowId, step) > 0);

            var fenced = new Outcome(world.eventStore().fencedInstance().orElse(""),
                                     rejectionsFor(rejections, workflowId),
                                     effects.count(workflowId, step),
                                     stepRecords(world.committedLog(), workflowId, step, StepStatus.STARTED),
                                     terminalWorkflowRecords(world.committedLog(), workflowId),
                                     0,
                                     0);

            // The next claim: the instance parked with nothing recorded for the step, so a restore has to run it.
            world.crashAndRecover();
            Polling.await(DEADLINE,
                          () -> stepRecords(world.committedLog(), workflowId, step, StepStatus.COMPLETED) >= 1);

            return new Outcome(fenced.fencedInstance(),
                               fenced.rejectionsObserved(),
                               fenced.effectRunsWhileFenced(),
                               fenced.startedRecordsWhileFenced(),
                               fenced.terminalRecordsWhileFenced(),
                               effects.count(workflowId, step),
                               stepRecords(world.committedLog(), workflowId, step, StepStatus.COMPLETED));
        } finally {
            rejections.close();
        }
    }

    private static int rejectionsFor(LogCapture rejections, String workflowId) {
        return (int) rejections.messages().stream()
                         .filter(message -> message != null && message.contains("was rejected")
                                 && message.contains("'" + workflowId + "'"))
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

    private static int terminalWorkflowRecords(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                         && MetadataUtils.getWorkflowStatus(event.metadata())
                                                         .filter(WorkflowStatus::isTerminal).isPresent())
                                 .count();
    }
}
