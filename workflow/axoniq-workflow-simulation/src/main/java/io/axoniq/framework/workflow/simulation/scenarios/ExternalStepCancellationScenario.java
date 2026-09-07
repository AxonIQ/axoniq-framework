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

import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ExternalCancelRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_AWAIT_APPROVAL;
import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_COMPENSATE;

/**
 * Deterministic scenario driving <strong>external step cancellation with catch-and-compensate</strong>: a running
 * blocking wait step is cancelled via {@code WorkflowExecution.cancelRunningStep(...)} from the scenario's thread —
 * NOT the workflow's own control thread — and the body must catch the surfaced {@link StepCancellationException},
 * run its compensation step to COMPLETED, and drive the workflow to a terminal COMPLETED status.
 * <p>
 * This is the off-control-thread half of the {@code cancelRunningStep} surface. The workflow's task queue has a single
 * consumer (the control thread); a cancellation must therefore be applied by <em>enqueueing</em> work for that
 * consumer. An implementation that instead pumps the task queue on the cancelling caller's thread races the control
 * thread for its own tasks and leaks an interrupt into it — observed as the compensation step starting but never
 * completing. The in-body cancellation path (a workflow cancelling its own step) is covered by the engine's regular
 * cancellation tests; this scenario pins the cross-thread path end to end.
 * <p>
 * Drive: publish the start event; wait until the blocking wait step is STARTED and its wait-timeout continuation is
 * queued on the virtual scheduler (so the step's running future is registered); then repeatedly invoke the external
 * cancel (idempotent — a no-op once the future is gone) until the step's CANCELLED record commits; finally wait for
 * the compensation step and the workflow terminal record. Scenario-pinned only (the registration is scenario-only, not
 * in {@link SimulationWorld} defaults): without an external cancel the blocking wait never resolves, so the always-on
 * liveness asserts would (correctly) flag it across the fuzz.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ExternalStepCancellationScenario {

    private ExternalStepCancellationScenario() {
    }

    /**
     * The observables the test asserts.
     *
     * @param awaitStepCancelled  whether the blocking wait step committed a terminal CANCELLED record.
     * @param compensateCompleted whether the compensation step committed a terminal COMPLETED record.
     * @param workflowCompleted   whether the workflow committed a terminal COMPLETED status record.
     * @param compensateEffects   how many times the compensation body's side effect ran (expected exactly 1).
     */
    public record Outcome(boolean awaitStepCancelled, boolean compensateCompleted, boolean workflowCompleted,
                          long compensateEffects) {

    }

    /**
     * Runs the external cancellation drive on a fresh world and returns the observed outcome.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.externalCancelWorkflow(effects))) {
            String workflowId = "extcancel-" + orderId;

            world.engine().publish(new ExternalCancelRequestedEvent(orderId));

            // The blocking wait step must be STARTED and its wait-timeout continuation queued on the virtual
            // scheduler before the external cancel can find a registered running future (the same readiness probe
            // BlockingAwaitTimeoutSurfaceScenario#runWaitPath uses).
            Polling.awaitOrFail(Duration.ofSeconds(10), "the blocking wait step to reach STARTED",
                                () -> hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL,
                                                    StepStatus.STARTED));
            Polling.awaitOrFail(Duration.ofSeconds(10), "the wait-timeout to be scheduled on the virtual scheduler",
                                () -> world.scheduler().pendingTasks() > 0);

            // THE EXTERNAL CANCEL — from this (the scenario's) thread, not the workflow control thread. Retried
            // inside the poll to close the tiny window between the STARTED record committing and the running future
            // registering; the call is idempotent once the future is cancelled.
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the externally cancelled wait step to commit a terminal CANCELLED record",
                                () -> {
                                    world.engine().cancelRunningStepOf(workflowId, STEP_AWAIT_APPROVAL,
                                                                       new StepCancellationException(
                                                                               "cancelled externally"));
                                    return hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL,
                                                         StepStatus.CANCELLED);
                                });

            // Catch-and-compensate must then complete: the body catches the StepCancellationException, runs the
            // compensation step to COMPLETED, and the workflow reaches a terminal COMPLETED status.
            Polling.awaitOrFail(Duration.ofSeconds(30),
                                "the compensation step to COMPLETE and the workflow to reach terminal COMPLETED",
                                () -> hasStepStatus(world.committedLog(), workflowId, STEP_COMPENSATE,
                                                    StepStatus.COMPLETED)
                                        && hasWorkflowStatus(world.committedLog(), workflowId,
                                                             WorkflowStatus.COMPLETED));

            return new Outcome(
                    hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED),
                    hasStepStatus(world.committedLog(), workflowId, STEP_COMPENSATE, StepStatus.COMPLETED),
                    hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED),
                    effects.count(workflowId, ExternalCancelCompensationWorkflow.STEP_COMPENSATE));
        }
    }

    // ---- log readers (per (workflowId, stepName), content-based) ----

    private static boolean hasStepStatus(List<EventMessage> committedLog, String workflowId,
                                         String stepName, StepStatus status) {
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
