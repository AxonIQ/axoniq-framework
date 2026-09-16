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

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.LaggingHistoryRepository;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ManagerProbeRequestedEvent;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.simulation.harness.EngineInstance.MANAGER_READ_TIMEOUT;
import static io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow.STEP_AWAIT_APPROVAL;
import static io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow.STEP_AWAIT_RESUME;
import static io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow.STEP_COMPENSATE;

/**
 * The manager's live-over-history rule, observed while the two sources disagree.
 * <p>
 * Exercises INV-36 {@code ManagerLiveWins}. Every settle-time oracle sees the live repository and the history
 * projection agree, so a merge that preferred history would pass them all (it did: canary (a) of the P7 campaign
 * escaped). This scenario makes the sources disagree on a live id and reads the manager in that state.
 * <p>
 * Drive: one {@code ManagerLiveWinsWorkflow} instance parks on its approval wait; once history holds that parked
 * state the manager-facing history read is frozen there ({@link LaggingHistoryRepository}). The wait is cancelled
 * through the manager, the body compensates and parks again on its resume wait — live state: approval CANCELLED,
 * compensate COMPLETED, resume STARTED, workflow STARTED; held history: approval STARTED only. The manager is read and
 * compared with the live execution's own state. The resume signal then completes the instance, the freeze is lifted,
 * and the manager is read once more to pin convergence.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ManagerLiveWinsScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(30);

    private ManagerLiveWinsScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param liveSteps      the live execution's step statuses while parked on the resume wait.
     * @param managerSteps   the manager's step statuses read at the same moment ({@code null} when nothing returned).
     * @param heldHistorySteps the frozen projection's step statuses at that moment, to prove the sources disagreed.
     * @param liveWinsHeld   whether INV-36 held on that read.
     * @param afterRelease   the manager's answer once the instance completed and the freeze was lifted.
     */
    public record Outcome(Map<String, StepStatus> liveSteps,
                          @Nullable Map<String, StepStatus> managerSteps,
                          Map<String, StepStatus> heldHistorySteps,
                          boolean liveWinsHeld,
                          @Nullable WorkflowStatus afterRelease) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the single instance.
     * @return the observed outcome.
     */
    public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var history = new LaggingHistoryRepository();
        var workflowId = "mgrlive-" + orderId;
        var query = WorkflowStateQuery.byWorkflowId(workflowId);
        try (var world = SimulationWorld.withHistoryRepository(
                seed, List.of(EngineInstance.managerLiveWinsWorkflow(effects)), history)) {
            world.engine().publish(new ManagerProbeRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the approval wait to park",
                                () -> hasStep(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL,
                                              StepStatus.STARTED));
            Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled",
                                () -> world.scheduler().pendingTasks() > 0);
            Polling.awaitOrFail(DEADLINE, "history to hold the parked state",
                                () -> FutureUtils.joinAndUnwrap(history.findById(workflowId))
                                                 .map(h -> h.state().containsStep(STEP_AWAIT_APPROVAL))
                                                 .orElse(false));
            history.hold();

            var one = world.engine().workflowManager().findOne(query);
            Polling.awaitOrFail(DEADLINE, "the approval wait to commit CANCELLED via the manager", () -> {
                FutureUtils.joinAndUnwrap(one.requestStepCancellation(
                        STEP_AWAIT_APPROVAL, new StepCancellationException("cancelled via manager"))
                                              .orTimeout(MANAGER_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                return hasStep(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED);
            });
            Polling.awaitOrFail(DEADLINE, "the body to compensate and park on the resume wait",
                                () -> hasStep(world.committedLog(), workflowId, STEP_COMPENSATE, StepStatus.COMPLETED)
                                        && hasStep(world.committedLog(), workflowId, STEP_AWAIT_RESUME,
                                                   StepStatus.STARTED)
                                        && world.engine().liveExecution(workflowId)
                                                .map(e -> e.state().containsStep(STEP_AWAIT_RESUME))
                                                .orElse(false));

            // THE READ: live and held history disagree; the manager must answer the live state.
            var live = world.engine().liveExecution(workflowId).orElseThrow().state();
            var liveSteps = steps(live);
            var answer = world.engine().managerSingleState(query);
            var heldSteps = FutureUtils.joinAndUnwrap(history.findAll(query)).stream()
                                       .findFirst().map(h -> steps(h.state())).orElse(Map.of());
            boolean liveWins;
            try {
                Invariants.assertManagerLiveWins(live, answer);
                liveWins = true;
            } catch (InvariantViolation violation) {
                liveWins = false;
            }

            world.engine().publish(new CorrelatedSignalEvent(orderId + ManagerLiveWinsWorkflow.RESUME_KEY_SUFFIX));
            Polling.awaitOrFail(DEADLINE, "the instance to complete",
                                () -> hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED));
            history.release();
            Polling.awaitOrFail(DEADLINE, "the manager to answer COMPLETED once the freeze is lifted",
                                () -> statusOf(world.engine().managerSingleState(query)) == WorkflowStatus.COMPLETED);
            return new Outcome(liveSteps, answer == null ? null : steps(answer), heldSteps, liveWins,
                               statusOf(world.engine().managerSingleState(query)));
        }
    }

    private static Map<String, StepStatus> steps(WorkflowState state) {
        var steps = new LinkedHashMap<String, StepStatus>();
        for (String name : state.workflowStepNames()) {
            var step = state.getStep(name);
            steps.put(name, step == null ? null : step.status());
        }
        return steps;
    }

    @Nullable
    private static WorkflowStatus statusOf(@Nullable WorkflowState state) {
        return state == null ? null : state.workflowStatus();
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId, String stepName,
                                   StepStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == status).orElse(false));
    }

    private static boolean hasWorkflowStatus(List<EventMessage> committedLog, String workflowId,
                                             WorkflowStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s == status).orElse(false));
    }
}
