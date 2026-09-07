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

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.AnyMatchNoMatchWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.AnyMatchNoMatchRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Deterministic scenario settling the candidate finding <strong>S-5</strong> and extending INVARIANTS.md INV-14
 * ({@code CombinatorConsistency}) coverage: the {@code anyMatch} winner-result semantics on the
 * no-predicate-match-but-all-branches-completed path. It drives the real engine through {@link AnyMatchNoMatchWorkflow}:
 * three {@code execute} branches that ALL complete but ALL vote {@code no} (so NONE satisfies the {@code VOTED_YES}
 * predicate), then reads the {@code anyMatch} winner-derived accessors and records them into the engine's reconstructed
 * payload.
 * <p>
 * The engine's {@code AnyMatchCombinatorDelegate.resolveWinner()} (runtime {@code AnyMatchCombinatorDelegate.java:73-117}):
 * with no predicate match but all branches completed, it takes the all-completed branch ({@code :83-89}) and sets the
 * first-completed branch ({@code fallback.orElse(results[0])}) as the winner. The winner-derived accessors then delegate
 * to that branch: {@code success()} reads its (COMPLETED) status, {@code result()}/{@code resultAs()} read its payload —
 * even though no branch matched the predicate. The predicate-level decision ({@code matched()} empty) is itself CORRECT.
 * <p>
 * OBSERVED behaviour (characterized, NOT a passing engine assertion — per the POC's test/docs-only rule):
 * <ul>
 *   <li>{@code anyMatch.matched()} is EMPTY (correct — the predicate-level decision is sound, consistent with INV-14);</li>
 *   <li>{@code anyMatch.unmatched().size() == 3} (all three completed-but-non-matching branches);</li>
 *   <li>{@code anyMatch.success()} is {@code true} — the winner-derived accessor reads the fallback (first completed)
 *       branch's COMPLETED status, even though NO branch matched the predicate (the misleading part);</li>
 *   <li>{@code anyMatch.result()} is PRESENT and returns {@link AnyMatchNoMatchWorkflow#BRANCH_A branch A}'s payload
 *       (the {@code fallback.orElse(results[0])} first-completed branch) — so {@code result()}/{@code resultAs()} read
 *       branch A's data as if it were "the winner".</li>
 * </ul>
 * Minor severity: it only bites an author who reads the winner-derived accessors on the no-match path (the
 * {@code matched()}/{@code unmatched()} categorization is the documented, correct way to read a no-match result).
 * Engine left unchanged; candidate fix: make the winner empty / the result accessors {@code Optional.empty()} explicitly
 * on the no-match path (rather than silently returning {@code results[0]}).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class AnyMatchNoMatchWinnerScenario {

    private AnyMatchNoMatchWinnerScenario() {
    }

    /**
     * Result of running the scenario — a CANDIDATE-FINDING characterization (S-5, POC-TLA-DST.adoc).
     *
     * @param reachedTerminal      whether the instance reached a terminal (COMPLETED) workflow status. OBSERVED:
     *                             {@code true} (the body is execute-only and completes on its own).
     * @param matchedEmpty         whether {@code anyMatch.matched()} is empty. OBSERVED: {@code true} — the predicate-level
     *                             decision is CORRECT (no branch matched), consistent with INV-14.
     * @param unmatchedSize        {@code anyMatch.unmatched().size()}. OBSERVED: {@code 3} — all completed-non-matching
     *                             branches.
     * @param anySuccess           the winner-delegated {@code anyMatch.success()}. OBSERVED: {@code true} — reads the
     *                             fallback (first completed) branch's COMPLETED status, even though no branch matched.
     * @param winnerResultPresent  whether {@code anyMatch.result()} is present and non-empty. OBSERVED: {@code true} —
     *                             the accessor returns the fallback branch's payload, NOT empty.
     * @param winnerBranchId       the {@code branchId} the winner-derived {@code anyMatch.result()} returns. OBSERVED:
     *                             {@link AnyMatchNoMatchWorkflow#BRANCH_A} — the {@code fallback.orElse(results[0])}
     *                             first-completed branch.
     */
    public record Outcome(boolean reachedTerminal, boolean matchedEmpty, int unmatchedSize, boolean anySuccess,
                          boolean winnerResultPresent, String winnerBranchId) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link AnyMatchNoMatchWorkflow} and
     * <strong>characterizes the engine's ACTUAL</strong> {@code anyMatch} winner-accessor behaviour on the
     * no-match-all-completed path.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.anyMatchNoMatchWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "anynomatch-" + orderId;

            // Start the workflow: three branches all complete voting "no", anyMatch resolves the no-match fallback winner,
            // and the body records the observed accessor values into the payload, then completes.
            world.engine().publish(new AnyMatchNoMatchRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10),
                                "the any-match-no-match instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            Map<String, Object> payload = reconstructedPayload(world, workflowId);

            return new Outcome(
                    isTerminal(world.committedLog(), workflowId),
                    asBoolean(payload.get(AnyMatchNoMatchWorkflow.KEY_ANY_MATCHED_EMPTY)),
                    asInt(payload.get(AnyMatchNoMatchWorkflow.KEY_ANY_UNMATCHED_SIZE)),
                    asBoolean(payload.get(AnyMatchNoMatchWorkflow.KEY_ANY_SUCCESS)),
                    asBoolean(payload.get(AnyMatchNoMatchWorkflow.KEY_WINNER_RESULT_PRESENT)),
                    String.valueOf(payload.getOrDefault(AnyMatchNoMatchWorkflow.KEY_WINNER_BRANCH_ID, "")));
        }
    }

    private static boolean asBoolean(Object value) {
        return Boolean.TRUE.equals(value) || "true".equals(String.valueOf(value));
    }

    private static int asInt(Object value) {
        return value == null ? -1 : Integer.parseInt(String.valueOf(value));
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata())
                                        .map(WorkflowStatus::isTerminal).orElse(false));
    }

        private static Map<String, Object> reconstructedPayload(SimulationWorld world,
                                                            String workflowId) {
        return world.engine().reconstructedPayloads("anynomatch-").getOrDefault(workflowId, Map.of());
    }
}
