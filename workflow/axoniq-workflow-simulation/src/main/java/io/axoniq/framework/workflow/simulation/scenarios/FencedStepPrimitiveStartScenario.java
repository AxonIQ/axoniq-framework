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
import io.axoniq.framework.workflow.simulation.harness.EngineInstance.WorkflowRegistration;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RetryingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.TimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.TimeoutWorkflow;

import java.time.Duration;
import java.util.function.Function;

/**
 * The {@code STARTED}-rejected gate, one step primitive at a time.
 * <p>
 * {@code FencedStepStartScenario} covers a plain {@code execute}. The gate is not per primitive though — every step
 * publishes a {@code STARTED} and waits for it before it does anything — so the primitives that carry their own
 * machinery have to be checked too: a retryable {@code execute} (whose retry policy could re-enter the gate), a
 * {@code waitForEvent} (whose scheduled timeout could fire without the gate ever passing) and a {@code sleep}.
 * <p>
 * Oracle, per primitive: the store holds no {@code STARTED} record for the step, the instance records nothing
 * terminal, and the step's own work never happens — no counted effect for an execute, no terminal step record for a
 * wait or a sleep. The rejection warning proves the append really did try.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedStepPrimitiveStartScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);

    private FencedStepPrimitiveStartScenario() {
    }

    /**
     * Outcome for one primitive.
     *
     * @param primitive        which primitive was fenced.
     * @param fencedInstance   the instance the fence wrote for; empty means the fence never fired.
     * @param startedRecords   {@code STARTED} records for the step (must be 0).
     * @param terminalStepRecords terminal records for the step (must be 0).
     * @param effectRuns       times the step's action ran (must be 0; always 0 for a wait or a sleep).
     * @param terminalRecords  terminal workflow records (must be 0).
     * @param rejections       rejection warnings for the instance (must be at least 1).
     */
    public record Outcome(String primitive, String fencedInstance, int startedRecords,
                          int terminalStepRecords, int effectRuns, int terminalRecords, int rejections) {

    }

    /**
     * Fences the {@code STARTED} of a retryable {@code execute} step.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance.
     * @return the observed outcome.
     */
        public static Outcome retryableExecute(long seed, String orderId) {
        return run("retryable execute", seed, "retry-" + orderId, RetryingWorkflow.STEP_FLAKY,
                   EngineInstance::retryingWorkflow, () -> new RetryRequestedEvent(orderId));
    }

    /**
     * Fences the {@code STARTED} of a {@code waitForEvent} step that carries a timeout.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance.
     * @return the observed outcome.
     */
        public static Outcome waitForEvent(long seed, String orderId) {
        return run("waitForEvent", seed, "timeout-" + orderId, TimeoutWorkflow.STEP_AWAIT_TIMEOUT,
                   EngineInstance::timeoutWorkflow, () -> new TimeoutRequestedEvent(orderId));
    }

    /**
     * Fences the {@code STARTED} of a {@code sleep} step.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance.
     * @return the observed outcome.
     */
        public static Outcome sleep(long seed, String orderId) {
        // The sleep sits behind a wait for the payment confirmation, so the body only reaches it once that is
        // delivered; the confirmation is re-published until the fence has consumed the sleep's STARTED commit.
        return run("sleep", seed, "order-" + orderId, OrderWorkflow.STEP_SETTLE_DELAY,
                   EngineInstance::orderWorkflow, () -> new OrderPlacedEvent(orderId),
                   world -> world.engine().publish(new PaymentConfirmedEvent(orderId)));
    }

        private static Outcome run(String primitive, long seed, String workflowId, String step,
                               Function<CountingEffects, WorkflowRegistration> registration,
                               java.util.function.Supplier<Object> startEvent) {
        return run(primitive, seed, workflowId, step, registration, startEvent, world -> {
        });
    }

        private static Outcome run(String primitive, long seed, String workflowId, String step,
                               Function<CountingEffects, WorkflowRegistration> registration,
                               java.util.function.Supplier<Object> startEvent,
                               java.util.function.Consumer<SimulationWorld> drive) {
        var effects = new CountingEffects();
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed, registration.apply(effects))) {
            world.eventStore().armForeignWriteBeforeCommitOf(step, StepStatus.STARTED);
            world.engine().publish(startEvent.get());

            Polling.awaitOrFail(DEADLINE, "the fence to consume the step's STARTED commit for " + primitive,
                                () -> {
                                    drive.accept(world);
                                    return !world.eventStore().isFenceArmed();
                                });
            var fencedInstance = world.eventStore().fencedInstance().orElse("");
            Polling.await(REJECTION_WINDOW, () -> FenceOracles.rejections(appender, workflowId) >= 1);
            // Give any late action invocation or scheduled continuation a window to show up.
            Polling.await(ABSENCE_WINDOW, () -> false);

            var log = world.committedLog();
            int terminalStepRecords = 0;
            for (StepStatus status : StepStatus.values()) {
                if (status.isTerminal()) {
                    terminalStepRecords += FenceOracles.stepRecords(log, workflowId, step, status);
                }
            }
            return new Outcome(primitive,
                               fencedInstance,
                               FenceOracles.stepRecords(log, workflowId, step, StepStatus.STARTED),
                               terminalStepRecords,
                               effects.count(workflowId, step),
                               FenceOracles.terminalRecords(log, workflowId),
                               FenceOracles.rejections(appender, workflowId));
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
