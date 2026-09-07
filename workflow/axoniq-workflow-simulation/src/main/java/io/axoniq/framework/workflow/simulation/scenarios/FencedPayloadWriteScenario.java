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
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PayloadOrderRequestedEvent;

import java.time.Duration;

/**
 * The append that gets rejected is a {@code modifyPayload} write.
 * <p>
 * A payload write is the one append whose in-memory effect exists before the record does: the body has already folded
 * the new keys into its own payload when the append goes out. If a rejected write's keys were to survive in memory and
 * ride out on a later accepted event, the durable log and the instance's payload would disagree — and the log is what
 * every later incarnation rebuilds from.
 * <p>
 * Oracle, in two halves. While fenced: nothing more is accepted for the instance. After the next claim restores it:
 * the engine's own reconstructed payload equals the fold of the committed log
 * ({@code Invariants.assertNoLostPayloadWrites}), so no rejected write leaked into an accepted one.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class FencedPayloadWriteScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final Duration REJECTION_WINDOW = Duration.ofSeconds(15);

    private FencedPayloadWriteScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param fencedInstance      the instance the fence wrote for; empty means the fence never fired.
     * @param recordsAfterFence   records the instance holds while fenced.
     * @param recordsBeforeFence  records it held when the fence fired.
     * @param rejections          rejection warnings for the instance (must be at least 1).
     * @param payloadFoldConsistent whether the restored instance's reconstructed payload equals the committed fold.
     */
    public record Outcome(String fencedInstance, int recordsBeforeFence, int recordsAfterFence,
                          int rejections, boolean payloadFoldConsistent) {

    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the instance whose payload write is fenced.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "payload-" + orderId;
        var appender = FenceOracles.attachRejectionAppender();
        try (var world = new SimulationWorld(seed, EngineInstance.payloadOrderWorkflow(effects))) {
            world.eventStore().armForeignWriteBeforeCommitOf(PayloadOrderWorkflow.STEP_FINALIZE_ORDER,
                                                             StepStatus.COMPLETED);
            world.engine().publish(new PayloadOrderRequestedEvent(orderId));

            Polling.awaitOrFail(DEADLINE, "the fence to consume the payload write's commit",
                                () -> !world.eventStore().isFenceArmed());
            var fencedInstance = world.eventStore().fencedInstance().orElse("");
            Polling.await(REJECTION_WINDOW, () -> FenceOracles.rejections(appender, workflowId) >= 1);
            int fencedRecords = FenceOracles.records(world.committedLog(), workflowId);
            Polling.await(Duration.ofSeconds(2), () -> false);
            int afterSettle = FenceOracles.records(world.committedLog(), workflowId);

            world.crashAndRecover();
            Polling.await(DEADLINE,
                          () -> FenceOracles.terminalRecords(world.committedLog(), workflowId) >= 1);

            boolean consistent;
            try {
                Invariants.assertNoLostPayloadWrites(world.committedLog(), "payload-",
                                                     world.engine().reconstructedPayloads("payload-"));
                consistent = true;
            } catch (RuntimeException violation) {
                consistent = false;
            }
            return new Outcome(fencedInstance, fencedRecords, afterSettle,
                               FenceOracles.rejections(appender, workflowId), consistent);
        } finally {
            FenceOracles.detach(appender);
        }
    }
}
