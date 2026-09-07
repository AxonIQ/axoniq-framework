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
package io.axoniq.framework.workflow.simulation.workflow;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;

import java.util.Map;
import java.util.function.Predicate;

/**
 * A workflow that runs three {@code execute} branches that ALL complete but NONE satisfies the combinator predicate, then
 * reads the {@code anyMatch} winner-derived accessors, used to characterize the candidate finding <strong>S-5</strong>:
 * on the no-predicate-match-but-all-branches-completed path, {@code AnyMatchCombinatorDelegate} sets
 * {@code fallback.orElse(results[0])} (the first completed branch) as the "winner" (runtime
 * {@code AnyMatchCombinatorDelegate.java:114-116}), so the winner-derived accessors ({@code success()}/{@code result()}/
 * {@code resultAs()}) read that branch's outcome even though it did NOT match the predicate.
 * <p>
 * This is the deliberate <em>no-match</em> companion of {@link CombinatorWorkflow} (which has A=yes so {@code anyMatch} is
 * matched). Here all three branches vote {@link #VOTE_NO no} under the {@link #VOTED_YES} predicate, so:
 * <ul>
 *   <li>{@code matched()} is EMPTY (correct — no branch satisfied the predicate);</li>
 *   <li>{@code resolveWinner()} falls into the all-completed branch ({@code AnyMatchCombinatorDelegate.java:83-89}) and
 *       sets the first-completed branch (declaration order here, since the branches are awaited in order) as the winner;</li>
 *   <li>so the winner-derived {@code success()} reads that branch's step status (COMPLETED ⇒ {@code true}), and
 *       {@code result()}/{@code resultAs()} read that branch's payload — even though no branch matched.</li>
 * </ul>
 * The body records, into the engine's reconstructed payload (via a {@code combine} {@code execute} step), the OBSERVED
 * winner-accessor values: {@link #KEY_ANY_SUCCESS} (the winner-delegated {@code success()}), {@link #KEY_ANY_MATCHED_EMPTY}
 * (whether {@code matched()} is empty — the predicate-level decision, which is CORRECT), {@link #KEY_ANY_UNMATCHED_SIZE},
 * {@link #KEY_WINNER_BRANCH_ID} (the {@link #KEY_BRANCH_ID} the winner-derived {@code result()} returns — pins WHICH branch
 * the accessor reads), and {@link #KEY_WINNER_RESULT_PRESENT}. The scenario/test then characterizes these against the
 * documented behaviour (a candidate finding, NOT a passing engine assertion — per the POC's test/docs-only rule).
 * <p>
 * Determinism (axon-flow-workflow skill §3.3): each branch returns a constant keyed only by its step name, awaited in
 * declaration order, so the committed subsequence — and therefore the winner the engine derives — is a pure function of
 * the history (a replay resolves the identical winner; the replay-stability facet INV-14 shares with INV-4).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class AnyMatchNoMatchWorkflow {

    /**
     * Logical workflow name (single definition), distinct from {@link CombinatorWorkflow}.
     */
    public static final String WORKFLOW_NAME = "AnyMatchNoMatchWorkflow";

    /**
     * Payload key each branch writes its vote under (mirrors {@link CombinatorWorkflow#KEY_VOTE}).
     */
    public static final String KEY_VOTE = "vote";

    /**
     * A per-branch identity key each branch writes (its own step name) so the winner-derived {@code result()} can be
     * traced back to WHICH branch the accessor returned.
     */
    public static final String KEY_BRANCH_ID = "branchId";

    /**
     * The vote value the predicate matches on — never produced here (all branches vote {@link #VOTE_NO}).
     */
    public static final String VOTE_YES = "yes";

    /**
     * The vote value all three branches produce (so NONE matches {@link #VOTED_YES}).
     */
    public static final String VOTE_NO = "no";

    /**
     * First parallel branch — votes {@link #VOTE_NO no}; the first-completed branch (declaration order), so the engine's
     * no-match fallback winner.
     */
    public static final String BRANCH_A = "branchA";

    /**
     * Second parallel branch — votes {@link #VOTE_NO no}.
     */
    public static final String BRANCH_B = "branchB";

    /**
     * Third parallel branch — votes {@link #VOTE_NO no}.
     */
    public static final String BRANCH_C = "branchC";

    /**
     * The step that records the observed winner-accessor values into the payload.
     */
    public static final String STEP_RECORD_OBSERVATION = "recordAnyMatchObservation";

    /**
     * Payload key recording the winner-delegated {@code anyMatch.success()} on the no-match-all-completed path. OBSERVED:
     * {@code true} (the fallback winner is a COMPLETED branch, so its {@code success()} is true) — even though no branch
     * matched the predicate.
     */
    public static final String KEY_ANY_SUCCESS = "anySuccess";

    /**
     * Payload key recording whether {@code anyMatch.matched()} is empty (the predicate-level decision). OBSERVED:
     * {@code true} — matched() is correctly empty (no branch satisfied the predicate).
     */
    public static final String KEY_ANY_MATCHED_EMPTY = "anyMatchedEmpty";

    /**
     * Payload key recording {@code anyMatch.unmatched().size()} — all three completed-but-non-matching branches.
     */
    public static final String KEY_ANY_UNMATCHED_SIZE = "anyUnmatchedSize";

    /**
     * Payload key recording the {@link #KEY_BRANCH_ID} the winner-derived {@code anyMatch.result()} returns — pins WHICH
     * branch's payload the accessor reads on the no-match path. OBSERVED: {@link #BRANCH_A} (the fallback {@code results[0]}
     * / first-completed branch).
     */
    public static final String KEY_WINNER_BRANCH_ID = "winnerBranchId";

    /**
     * Payload key recording whether {@code anyMatch.result()} is present (a non-empty payload) on the no-match path.
     * OBSERVED: {@code true} — the accessor returns the fallback branch's payload, NOT empty.
     */
    public static final String KEY_WINNER_RESULT_PRESENT = "winnerResultPresent";

    /**
     * The combinator predicate: a branch result whose committed payload voted {@link #VOTE_YES yes} — never true here.
     */
    public static final Predicate<WorkflowStepResult> VOTED_YES =
            r -> r.result().map(m -> VOTE_YES.equals(m.get(KEY_VOTE))).orElse(false);

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public AnyMatchNoMatchWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: launch three branch {@code execute} steps that all complete voting {@link #VOTE_NO no}, fold them through
     * {@code anyMatch} over the {@link #VOTED_YES} predicate (no branch matches), then read the winner-derived accessors
     * and record them into the payload. Completes on its own (execute-only).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        WorkflowStepResult branchA = vote(ctx, workflowId, BRANCH_A);
        WorkflowStepResult branchB = vote(ctx, workflowId, BRANCH_B);
        WorkflowStepResult branchC = vote(ctx, workflowId, BRANCH_C);

        // anyMatch over a predicate NO branch satisfies, but all branches completed: matched() is empty, yet the engine
        // sets fallback.orElse(results[0]) (the first completed branch) as the winner, so the winner-derived accessors
        // read that branch. Read all accessors AFTER await() (skill §4).
        CombinatorWorkflowStepResult any = ctx.anyMatch(VOTED_YES, branchA, branchB, branchC);
        any.await();

        boolean anySuccess = any.success();                       // winner-delegated: the fallback branch's success()
        boolean matchedEmpty = any.matched().isEmpty();           // predicate-level decision (correct: empty)
        int unmatchedSize = any.unmatched().size();
        Map<String, Object> winnerResult = any.result().orElse(Map.of());
        boolean winnerResultPresent = any.result().isPresent() && !winnerResult.isEmpty();
        String winnerBranchId = String.valueOf(winnerResult.getOrDefault(KEY_BRANCH_ID, ""));

        ctx.awaitExecute(
                STEP_RECORD_OBSERVATION,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_RECORD_OBSERVATION);
                    return Map.of(
                            KEY_ANY_SUCCESS, anySuccess,
                            KEY_ANY_MATCHED_EMPTY, matchedEmpty,
                            KEY_ANY_UNMATCHED_SIZE, unmatchedSize,
                            KEY_WINNER_BRANCH_ID, winnerBranchId,
                            KEY_WINNER_RESULT_PRESENT, winnerResultPresent);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
    }

    /**
     * One branch: an {@code execute} step that bumps the effect counter and returns its constant {@link #VOTE_NO no} vote
     * plus its {@link #KEY_BRANCH_ID identity} (merged into the payload via {@code CombineGlobalAndLocalPayloadReducer}).
     * Awaited before returning so it reaches its terminal outcome (and commits) in declaration order.
     */
        private WorkflowStepResult vote(SimpleWorkflowContext ctx, String workflowId,
                                    String branch) {
        WorkflowStepResult result = ctx.execute(
                branch,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, branch);
                    return Map.of(KEY_VOTE, VOTE_NO, KEY_BRANCH_ID, branch);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
        result.await();
        return result;
    }
}
