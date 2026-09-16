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
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.LaggingHistoryRepository;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ExternalCancelRequestedEvent;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_AWAIT_APPROVAL;

/**
 * The Workflow Manager's two-source read observed across the window in which a finished execution has left the live
 * repository while the history projection has not yet caught up.
 * <p>
 * Bridges TLA+ counterexamples {@code MC_managerview.cfg} / {@code ManagerVisibilityMonotonic} (INV-31, F-42) and
 * {@code MC_managerview_status.cfg} / {@code ManagerStatusMonotonic} (INV-32, F-42). The model's lag is the
 * projector's position behind the log; the harness reproduces it deterministically with a
 * {@link LaggingHistoryRepository} that freezes the manager-facing history read at a chosen point while the projector
 * keeps writing. The live half is the real engine: {@code WorkflowEngine.removeExecution} drops the execution the
 * moment its body finishes.
 * <p>
 * Drive: one {@code ExternalCancelCompensationWorkflow} instance is started and parks on its approval wait. In
 * {@link Mode#VISIBILITY} the history read is frozen before the start, so history knows nothing about the instance;
 * in {@link Mode#STATUS} it is frozen once history holds the STARTED state. The manager is asked for the instance
 * while it is live (the answer it must never take back), the approval signal completes the body, the harness waits
 * for the execution to leave the live set, and the manager is asked again. The freeze is then lifted and the manager
 * is asked a third time, to pin that the answer converges.
 * <p>
 * Oracle: the second answer, compared with the first. In VISIBILITY it is {@code null} — an id the manager already
 * answered has vanished. In STATUS it is {@code STARTED} although the log holds COMPLETED — the answer moved backwards.
 * The third answer is COMPLETED in both modes. {@link Mode#UNHELD} runs the same drive without a freeze and only
 * counts how many of the polled answers between completion and convergence were absent or stale, as a measurement of
 * the real window on this machine; it asserts nothing about them.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ManagerVisibilityProbeScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);

    private ManagerVisibilityProbeScenario() {
    }

    /**
     * Which lag the scenario induces.
     */
    public enum Mode {
        /** History frozen before the instance exists: the manager forgets a finished id. */
        VISIBILITY,
        /** History frozen at the STARTED state: the manager answers STARTED for a completed id. */
        STATUS,
        /** No freeze: the real projector lag is measured, not induced. */
        UNHELD
    }

    /**
     * Outcome of the scenario.
     *
     * @param whileLive           the manager's answer while the instance was live and parked (expected STARTED).
     * @param afterRemoval        the manager's answer once the finished execution left the live set, under the
     *                            induced lag ({@code null} when the manager returned nothing).
     * @param afterRelease        the manager's answer once the lag was lifted and the projection caught up.
     * @param logStatus           the workflow status the committed log folds to when {@code afterRemoval} was read.
     * @param staleAnswersUnheld  in {@link Mode#UNHELD}, how many polled answers between completion and convergence
     *                            were absent or non-terminal; {@code 0} in the held modes.
     */
    public record Outcome(@Nullable WorkflowStatus whileLive,
                          @Nullable WorkflowStatus afterRemoval,
                          @Nullable WorkflowStatus afterRelease,
                          @Nullable WorkflowStatus logStatus,
                          int staleAnswersUnheld) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the single instance.
     * @param mode    which lag to induce.
     * @return the observed outcome.
     */
    public static Outcome run(long seed, String orderId, Mode mode) {
        var effects = new CountingEffects();
        var history = new LaggingHistoryRepository();
        var workflowId = "extcancel-" + orderId;
        var query = WorkflowStateQuery.byWorkflowId(workflowId);
        try (var world = SimulationWorld.withHistoryRepository(
                seed, List.of(EngineInstance.externalCancelWorkflow(effects)), history)) {
            if (mode == Mode.VISIBILITY) {
                history.hold();
            }
            world.engine().publish(new ExternalCancelRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the approval wait to park",
                                () -> hasStepStatus(world.committedLog(), workflowId, StepStatus.STARTED));
            if (mode == Mode.STATUS) {
                Polling.awaitOrFail(DEADLINE, "history to hold the STARTED state",
                                    () -> FutureUtils.joinAndUnwrap(history.findById(workflowId)).isPresent());
                history.hold();
            }
            var whileLive = statusOf(world.engine().managerSingleState(query));

            world.engine().publish(new CorrelatedSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the body to finish and leave the live set",
                                () -> hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED)
                                        && !world.engine().liveWorkflowIds().contains(workflowId));

            int stale = 0;
            WorkflowStatus afterRemoval;
            if (mode == Mode.UNHELD) {
                var stateNow = world.engine().managerSingleState(query);
                afterRemoval = statusOf(stateNow);
                if (afterRemoval != WorkflowStatus.COMPLETED) {
                    stale++;
                }
                var counter = new int[]{stale};
                Polling.awaitOrFail(DEADLINE, "the manager to converge on COMPLETED", () -> {
                    var status = statusOf(world.engine().managerSingleState(query));
                    if (status != WorkflowStatus.COMPLETED) {
                        counter[0]++;
                        return false;
                    }
                    return true;
                });
                stale = counter[0];
            } else {
                afterRemoval = statusOf(world.engine().managerSingleState(query));
            }
            var logStatus = foldStatus(world.committedLog(), workflowId);

            history.release();
            Polling.awaitOrFail(DEADLINE, "the manager to answer COMPLETED once the lag is lifted",
                                () -> statusOf(world.engine().managerSingleState(query)) == WorkflowStatus.COMPLETED);
            var afterRelease = statusOf(world.engine().managerSingleState(query));
            return new Outcome(whileLive, afterRemoval, afterRelease, logStatus, stale);
        }
    }

    @Nullable
    private static WorkflowStatus statusOf(@Nullable WorkflowState state) {
        return state == null ? null : state.workflowStatus();
    }

    @Nullable
    private static WorkflowStatus foldStatus(List<EventMessage> committedLog, String workflowId) {
        WorkflowStatus status = null;
        for (EventMessage event : committedLog) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                status = MetadataUtils.getWorkflowStatus(event.metadata()).orElse(status);
            }
        }
        return status;
    }

    private static boolean hasStepStatus(List<EventMessage> committedLog, String workflowId, StepStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && STEP_AWAIT_APPROVAL.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == status).orElse(false));
    }

    private static boolean hasWorkflowStatus(List<EventMessage> committedLog, String workflowId,
                                             WorkflowStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s == status).orElse(false));
    }
}
