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

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PayloadOrderRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Deterministic scenario for INVARIANTS.md INV-13 ({@code NoLostPayloadWrites}): a workflow whose every step writes a
 * distinct key to the payload finishes with a final committed payload that reflects <strong>every</strong> committed
 * step's contribution, and a crash + replay rebuilds the <strong>identical</strong> payload — no committed payload
 * write is lost across the recovery.
 * <p>
 * It drives {@link PayloadOrderWorkflow} (registered as a single definition via
 * {@link EngineInstance#payloadOrderWorkflow}). A fresh start runs three {@code execute} steps that each merge a
 * distinct key into the payload (via {@code CombineGlobalAndLocalPayloadReducer.INSTANCE}) and one {@code modifyPayload}
 * step that adds a fourth key while preserving the earlier three.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs to COMPLETED, recording the four distinct payload keys
 *       ({@link PayloadOrderWorkflow#KEY_INVENTORY_RESERVED}, {@link PayloadOrderWorkflow#KEY_PAYMENT_CHARGED},
 *       {@link PayloadOrderWorkflow#KEY_SHIPMENT_RECORDED}, {@link PayloadOrderWorkflow#KEY_ORDER_FINALIZED});</li>
 *   <li>rebuild the final committed payload (exactly as the engine evolves it — see
 *       {@link Invariants#rebuildPayload}); {@link Invariants#assertNoLostPayloadWrites} must already hold and the
 *       payload must carry all four keys;</li>
 *   <li>crash + recover (drives the real replay path), then assert the rebuilt payload is byte-for-byte unchanged —
 *       replay rebuilt the identical payload, no committed contribution lost.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class NoLostPayloadWritesScenario {

    private NoLostPayloadWritesScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal      whether the instance reached a terminal (COMPLETED) workflow status.
     * @param payloadBeforeCrash   the engine's reconstructed final payload at the terminal point (the history
     *                             read-model's {@code state().payload()}). INV-13 requires it to carry every step's
     *                             contribution.
     * @param payloadAfterCrash    the engine's reconstructed final payload after a crash + replay. INV-13's
     *                             replay-stability facet requires it to equal {@code payloadBeforeCrash} (replay
     *                             rebuilds the identical payload, no committed write lost).
     */
    public record Outcome(boolean reachedTerminal, Map<String, Object> payloadBeforeCrash,
                          Map<String, Object> payloadAfterCrash) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link PayloadOrderWorkflow} and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.payloadOrderWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "payload-" + orderId;

            // 1. Start the workflow: a fresh start runs the body (three execute+combine merges then one modifyPayload
            // replace) and completes on its own (execute/modifyPayload-only).
            world.engine().publish(new PayloadOrderRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "payload instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Read the engine's reconstructed final payload at the terminal point. INV-13 must already hold (every
            // committed contribution reflected in the engine's payload) and all four keys its steps wrote must be there.
            Map<String, Object> payloadBefore = reconstructedPayload(world, workflowId);
            Invariants.assertNoLostPayloadWrites(world.committedLog(), "payload-",
                                                 world.engine().reconstructedPayloads("payload-"));

            // 3. IN-SCOPE INV-13 (replay-stability): a crash + replay must rebuild the SAME payload — every committed
            // contribution survives, none lost or dropped across the recovery.
            world.crashAndRecover();
            // Give replay a bounded window to (potentially) rebuild a different payload before reading it back.
            Polling.await(Duration.ofSeconds(2),
                          () -> !reconstructedPayload(world, workflowId).equals(payloadBefore));
            Map<String, Object> payloadAfter = reconstructedPayload(world, workflowId);
            Invariants.assertNoLostPayloadWrites(world.committedLog(), "payload-",
                                                 world.engine().reconstructedPayloads("payload-"));

            return new Outcome(true, payloadBefore, payloadAfter);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * The engine's own reconstructed final payload for {@code workflowId} (the history read-model's
     * {@code state().payload()}), or an empty map if it has not been projected yet.
     */
        private static Map<String, Object> reconstructedPayload(SimulationWorld world,
                                                            String workflowId) {
        return world.engine().reconstructedPayloads("payload-").getOrDefault(workflowId, Map.of());
    }
}
