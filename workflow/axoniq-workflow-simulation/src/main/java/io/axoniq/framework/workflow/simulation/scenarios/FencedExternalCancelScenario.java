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
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ExternalCancelRequestedEvent;

import java.time.Duration;

/**
 * An external cancel is issued to a node that has already lost the instance.
 * <p>
 * Cancellation enters the engine off the workflow's own thread and drives a terminal transition. It is the one path
 * where a stale node is told to end an instance by something other than its own body, so it is the one most likely to
 * record an outcome the current owner never chose. The foreign write lands while the instance is parked on its
 * approval wait, and the cancel arrives after it.
 * <p>
 * Oracle: nothing more is accepted for the instance — same record count before and after, no {@code CANCELLED} step
 * record, no terminal workflow record — with the rejection warning as the proof the cancel really did try to append.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedExternalCancelScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);

    private FencedExternalCancelScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param recordsBeforeFence records the instance held when the foreign write landed.
     * @param recordsAfterFence  records it holds after the external cancel ran on the fenced node.
     * @param cancelledRecords   {@code CANCELLED} records for the wait step (must be 0).
     * @param terminalRecords    terminal workflow records (must be 0).
     * @param rejections         rejection warnings for the instance (must be at least 1).
     */
    public record Outcome(int recordsBeforeFence, int recordsAfterFence, int cancelledRecords,
                          int terminalRecords, int rejections) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance that is fenced and then cancelled.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "extcancel-" + orderId;
        var step = ExternalCancelCompensationWorkflow.STEP_AWAIT_APPROVAL;
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed, EngineInstance.externalCancelWorkflow(effects))) {
            world.engine().publish(new ExternalCancelRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the approval wait to park",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId, step,
                                                               StepStatus.STARTED) >= 1);
            Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled on the virtual scheduler",
                                () -> world.scheduler().pendingTasks() > 0);

            int before = FenceOracles.records(world.committedLog(), workflowId);
            world.eventStore().appendForeign(workflowId);

            // The cancel is idempotent until the step future is gone, so it is retried until the fence answers.
            Polling.await(REJECTION_WINDOW, () -> {
                world.engine().cancelRunningStepOf(workflowId, step,
                                                   new StepCancellationException("cancelled externally"));
                return FenceOracles.rejections(appender, workflowId) >= 1;
            });
            return new Outcome(before,
                               FenceOracles.records(world.committedLog(), workflowId),
                               FenceOracles.stepRecords(world.committedLog(), workflowId, step, StepStatus.CANCELLED),
                               FenceOracles.terminalRecords(world.committedLog(), workflowId),
                               FenceOracles.rejections(appender, workflowId));
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
