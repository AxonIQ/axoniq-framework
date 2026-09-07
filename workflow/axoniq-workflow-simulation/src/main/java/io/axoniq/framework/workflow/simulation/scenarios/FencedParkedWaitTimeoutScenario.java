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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.TimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.TimeoutWorkflow;

import java.time.Duration;

/**
 * A foreign write lands while the instance is parked on a {@code waitForEvent}, and then virtual time reaches that
 * wait's timeout on the node that no longer owns it.
 * <p>
 * A parked instance appends nothing, so nothing can fence it while it waits. Its timeout is the append it wakes up to
 * make, and that one has to be rejected: a timeout terminal from a node another writer has overtaken would record an
 * outcome the winner never chose.
 * <p>
 * Oracle: nothing more is accepted for the instance after the foreign write — same record count before and after,
 * no {@code TIMED_OUT} record and no terminal workflow record — with the rejection warning as the proof the timeout
 * really did try.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedParkedWaitTimeoutScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(10);

    private FencedParkedWaitTimeoutScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param recordsBeforeFence records the instance held when the foreign write landed.
     * @param recordsAfterFence  records it holds after the timeout fired on the fenced node.
     * @param timedOutRecords    {@code TIMED_OUT} records for the wait step (must be 0).
     * @param terminalRecords    terminal workflow records (must be 0).
     * @param rejections         rejection warnings for the instance (must be at least 1).
     */
    public record Outcome(int recordsBeforeFence, int recordsAfterFence, int timedOutRecords,
                          int terminalRecords, int rejections) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance that is parked and then fenced.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "timeout-" + orderId;
        var step = TimeoutWorkflow.STEP_AWAIT_TIMEOUT;
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed, EngineInstance.timeoutWorkflow(effects))) {
            world.engine().publish(new TimeoutRequestedEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the wait step to park",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId, step,
                                                               StepStatus.STARTED) >= 1);
            Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled on the virtual scheduler",
                                () -> world.scheduler().pendingTasks() > 0);

            int before = FenceOracles.records(world.committedLog(), workflowId);
            // Land the foreign write on the timeout's own commit. A blind write while the instance is parked is the
            // same adversary, but whether the scheduled continuation then fires within a bounded window is a race the
            // scenario cannot settle: measured over three runs it produced a rejection once, with the safety oracle
            // holding all three times. Arming the commit makes the same fence deterministic.
            world.eventStore().armForeignWriteBeforeCommitOf(step, StepStatus.TIMED_OUT);

            // Advance to the deadline the engine computed, not by a fixed delta: the recorded STARTED timestamp comes
            // from the event-store clock while the harness's virtual clock starts at the epoch, so the two sit in
            // different eras and a fixed advance never crosses the deadline.
            var started = startedAt(world.committedLog(), workflowId, step).orElseThrow();
            var fireBy = started.plus(TimeoutWorkflow.AWAIT_TIMEOUT).plusSeconds(1);
            var advance = Duration.between(world.clock().instant(), fireBy);
            world.advanceTime(advance.isNegative() ? Duration.ZERO : advance);

            Polling.await(REJECTION_WINDOW, () -> FenceOracles.rejections(appender, workflowId) >= 1);
            return new Outcome(before,
                               FenceOracles.records(world.committedLog(), workflowId),
                               FenceOracles.stepRecords(world.committedLog(), workflowId, step, StepStatus.TIMED_OUT),
                               FenceOracles.terminalRecords(world.committedLog(), workflowId),
                               FenceOracles.rejections(appender, workflowId));
        } finally {
            FenceOracles.detach(appender);
        }
    }

    /**
     * Returns the timestamp the given step's {@code STARTED} record carries — the anchor the engine computes the wait
     * deadline from.
     */
        private static java.util.Optional<java.time.Instant> startedAt(
            java.util.List<org.axonframework.messaging.eventhandling.EventMessage> log,
            String workflowId,
            String stepName) {
        return log.stream()
                  .filter(event -> workflowId.equals(
                          io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowId(event.metadata()))
                          && stepName.equals(
                          io.axoniq.framework.workflow.runtime.util.MetadataUtils.getStepName(event.metadata()))
                          && io.axoniq.framework.workflow.runtime.util.MetadataUtils.getStepStatus(event.metadata())
                                                                          .filter(s -> s == StepStatus.STARTED)
                                                                          .isPresent())
                  .map(org.axonframework.messaging.eventhandling.EventMessage::timestamp)
                  .findFirst();
    }
}
