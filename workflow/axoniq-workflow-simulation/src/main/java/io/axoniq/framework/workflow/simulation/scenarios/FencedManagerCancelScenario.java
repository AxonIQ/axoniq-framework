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
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ExternalCancelRequestedEvent;
import org.axonframework.common.FutureUtils;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.simulation.harness.EngineInstance.MANAGER_READ_TIMEOUT;

/**
 * A cancellation requested through the {@code WorkflowManager} on a node that has already lost the instance.
 * <p>
 * The manager twin of {@link FencedExternalCancelScenario}: same foreign write while the instance is parked on its
 * approval wait, same cancel afterwards, but issued through {@code findOne(byWorkflowId).requestStepCancellation}.
 * The manager resolves the target from the live repository, which on the stale node still holds the instance, so the
 * request reaches the execution and the fence has to do the work.
 * <p>
 * Oracle: nothing more is accepted for the instance — same record count before and after, no {@code CANCELLED} step
 * record, no terminal workflow record — with the rejection warning as the proof the cancel really did try to append.
 * The last answer of the request is recorded as well: the API documents {@code true} as "a terminal step cancellation
 * was recorded", and a fenced node records nothing.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedManagerCancelScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);

    private FencedManagerCancelScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param recordsBeforeFence records the instance held when the foreign write landed.
     * @param recordsAfterFence  records it holds after the manager cancel ran on the fenced node.
     * @param cancelledRecords   {@code CANCELLED} records for the wait step (must be 0).
     * @param terminalRecords    terminal workflow records (must be 0).
     * @param rejections         rejection warnings for the instance (must be at least 1).
     * @param lastAnswer         the last {@code requestStepCancellation} answer the fenced node gave.
     */
    public record Outcome(int recordsBeforeFence, int recordsAfterFence, int cancelledRecords,
                          int terminalRecords, int rejections, @Nullable Boolean lastAnswer) {

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

            var one = world.engine().workflowManager().findOne(WorkflowStateQuery.byWorkflowId(workflowId));
            var answer = new Boolean[1];
            // The cancel is idempotent until the step future is gone, so it is retried until the fence answers.
            Polling.await(REJECTION_WINDOW, () -> {
                answer[0] = FutureUtils.joinAndUnwrap(
                        one.requestStepCancellation(step, new StepCancellationException("cancelled via manager"))
                           .orTimeout(MANAGER_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                return FenceOracles.rejections(appender, workflowId) >= 1;
            });
            return new Outcome(before,
                               FenceOracles.records(world.committedLog(), workflowId),
                               FenceOracles.stepRecords(world.committedLog(), workflowId, step, StepStatus.CANCELLED),
                               FenceOracles.terminalRecords(world.committedLog(), workflowId),
                               FenceOracles.rejections(appender, workflowId),
                               answer[0]);
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
