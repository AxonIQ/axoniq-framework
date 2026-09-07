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
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OnRetryFires;
import io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryEdgesRequestedEvent;

import java.time.Duration;

/**
 * A foreign write lands while a step is {@code RETRYING} and waiting out its backoff, and the retry then fires on the
 * node that no longer owns the instance.
 * <p>
 * A backoff window is the longest an execute step sits idle while still owning work it intends to record. The retry
 * that comes out of it runs the action again and appends its outcome, so it is the append that must be rejected —
 * and, because the run's own {@code STARTED} for that attempt is what the store has to accept first, the action must
 * not run either.
 * <p>
 * Oracle: nothing more is accepted for the instance after the foreign write, the step's effect counter does not move,
 * and the rejection warning proves the retry really did try.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedRetryBackoffScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);

    private FencedRetryBackoffScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param recordsBeforeFence records the instance held when the foreign write landed.
     * @param recordsAfterFence  records it holds after the backoff elapsed on the fenced node.
     * @param effectsBeforeFence times the step's action had run when the foreign write landed.
     * @param effectsAfterFence  times it has run after the backoff elapsed.
     * @param terminalRecords    terminal workflow records (must be 0).
     * @param rejections         rejection warnings for the instance (must be at least 1).
     */
    public record Outcome(int recordsBeforeFence, int recordsAfterFence, int effectsBeforeFence,
                          int effectsAfterFence, int terminalRecords, int rejections) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance whose retry is fenced.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "retryedge-" + orderId;
        var step = RetryTimingWorkflow.STEP_FIXED;
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed,
                                             EngineInstance.retryTimingRetryEdgesWorkflow(effects,
                                                                                          new OnRetryFires()))) {
            world.engine().publish(new RetryEdgesRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the step to record its first RETRYING attempt",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId, step,
                                                               StepStatus.RETRYING) >= 1);

            int recordsBefore = FenceOracles.records(world.committedLog(), workflowId);
            int effectsBefore = effects.count(workflowId, step);
            world.eventStore().appendForeign(workflowId);

            // Let the backoff elapse so the retry attempt actually fires on the fenced node.
            Polling.await(REJECTION_WINDOW, () -> {
                world.advanceTime(RetryTimingWorkflow.FIXED_DELAY.multipliedBy(2));
                return FenceOracles.rejections(appender, workflowId) >= 1;
            });
            return new Outcome(recordsBefore,
                               FenceOracles.records(world.committedLog(), workflowId),
                               effectsBefore,
                               effects.count(workflowId, step),
                               FenceOracles.terminalRecords(world.committedLog(), workflowId),
                               FenceOracles.rejections(appender, workflowId));
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
