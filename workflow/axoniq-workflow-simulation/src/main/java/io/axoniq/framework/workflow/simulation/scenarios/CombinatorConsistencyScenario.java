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
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Deterministic scenario for INVARIANTS.md INV-14 ({@code CombinatorConsistency}): a workflow that runs three parallel
 * {@code execute} branches and folds them through all three combinators finishes with each combinator's decision
 * consistent with the documented short-circuit semantics over its branches' committed terminal outcomes, and a crash +
 * replay rebuilds the <strong>identical</strong> decisions — no combinator resolves a different decision across the
 * recovery.
 * <p>
 * It drives {@link CombinatorWorkflow} (registered as a single definition via {@link EngineInstance#combinatorWorkflow}).
 * A fresh start runs three branches ({@link CombinatorWorkflow#BRANCH_A A} votes yes, {@link CombinatorWorkflow#BRANCH_B
 * B} and {@link CombinatorWorkflow#BRANCH_C C} vote no), so the documented semantics fix every decision: {@code anyMatch}
 * matched, {@code allMatch} unmatched, {@code noneMatch} unmatched.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs to COMPLETED, recording each combinator's decision as a distinct
 *       post-combinator step name and as a payload key (merged into the payload);</li>
 *   <li>snapshot the combinator decisions (the recorded post-combinator steps' presence + the reconstructed-payload
 *       booleans); {@link Invariants#assertCombinatorConsistency} must already hold;</li>
 *   <li>crash + recover (drives the real replay path), then assert the decisions are byte-for-byte unchanged — replay
 *       resolved the SAME combinator decisions (deterministic across replay), still consistent with the branch
 *       outcomes.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class CombinatorConsistencyScenario {

    private CombinatorConsistencyScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal       whether the instance reached a terminal (COMPLETED) workflow status.
     * @param decisionsBeforeCrash  the recorded combinator decisions at the terminal point, keyed
     *                              {@code anyMatch}/{@code allMatch}/{@code noneMatch} (the engine's reconstructed-payload
     *                              boolean for each). INV-14 requires these to match the documented semantics over the
     *                              branch outcomes ({@code anyMatch=true}, {@code allMatch=false}, {@code noneMatch=false}).
     * @param decisionsAfterCrash   the same recorded decisions after a crash + replay. INV-14's deterministic-across-replay
     *                              facet requires this to equal {@code decisionsBeforeCrash}.
     * @param anyMatchWinner        the recorded {@code anyMatch} winner step name (must be the single matching branch A).
     */
    public record Outcome(boolean reachedTerminal, Map<String, Object> decisionsBeforeCrash,
                          Map<String, Object> decisionsAfterCrash, String anyMatchWinner) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link CombinatorWorkflow} and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.combinatorWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "comb-" + orderId;

            // 1. Start the workflow: a fresh start runs the three branches then the three combinators and completes on
            // its own (execute-only).
            world.engine().publish(new CombinatorRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "combinator instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Snapshot the recorded combinator decisions at the terminal point. INV-14 must already hold (each
            // recorded decision consistent with the branch outcomes).
            Map<String, Object> decisionsBefore = recordedDecisions(world, workflowId);
            String winner = String.valueOf(reconstructedPayload(world, workflowId)
                                                    .getOrDefault(CombinatorWorkflow.KEY_ANY_WINNER, ""));
            Invariants.assertCombinatorConsistency(world.committedLog(), "comb-",
                                                   world.engine().reconstructedPayloads("comb-"));

            // 3. IN-SCOPE INV-14 (deterministic across replay): a crash + replay must resolve the SAME combinator
            // decisions. Replay re-runs the (already-recorded) body and emits nothing new; the decisions must be
            // unchanged.
            world.crashAndRecover();
            // Give replay a bounded window to (potentially) rebuild different decisions before reading them back.
            Polling.await(Duration.ofSeconds(2),
                          () -> !recordedDecisions(world, workflowId).equals(decisionsBefore));
            Map<String, Object> decisionsAfter = recordedDecisions(world, workflowId);
            Invariants.assertCombinatorConsistency(world.committedLog(), "comb-",
                                                   world.engine().reconstructedPayloads("comb-"));

            return new Outcome(true, decisionsBefore, decisionsAfter, winner);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * The recorded combinator decisions for {@code workflowId}, read from the engine's reconstructed payload (the
     * history read-model's {@code state().payload()}), keyed {@code anyMatch}/{@code allMatch}/{@code noneMatch}. An
     * absent decision is rendered as {@code null} so the before/after comparison is stable.
     */
        private static Map<String, Object> recordedDecisions(SimulationWorld world, String workflowId) {
        var payload = reconstructedPayload(world, workflowId);
        var decisions = new TreeMap<String, Object>();
        decisions.put("anyMatch", payload.get(CombinatorWorkflow.KEY_ANY_MATCHED));
        decisions.put("allMatch", payload.get(CombinatorWorkflow.KEY_ALL_MATCHED));
        decisions.put("noneMatch", payload.get(CombinatorWorkflow.KEY_NONE_MATCHED));
        return decisions;
    }

    /**
     * The engine's own reconstructed final payload for {@code workflowId} (the history read-model's
     * {@code state().payload()}), or an empty map if it has not been projected yet.
     */
        private static Map<String, Object> reconstructedPayload(SimulationWorld world,
                                                            String workflowId) {
        return world.engine().reconstructedPayloads("comb-").getOrDefault(workflowId, Map.of());
    }
}
