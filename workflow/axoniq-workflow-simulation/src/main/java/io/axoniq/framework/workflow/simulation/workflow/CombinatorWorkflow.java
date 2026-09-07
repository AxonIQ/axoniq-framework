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
 * A workflow that runs three {@code execute} branch steps and then folds them through all three combinators
 * ({@code anyMatch}, {@code allMatch}, {@code noneMatch}), used to exercise INVARIANTS.md INV-14
 * ({@code CombinatorConsistency}): a combinator's decision is consistent with the documented short-circuit semantics
 * (axon-flow-workflow skill §4 "Combinators", §7.2/§7.3) and is a pure function of its branch steps' committed terminal
 * outcomes — stable across crash/replay.
 * <p>
 * Branch design (deterministic, payload-driven — <strong>not</strong> randomness — so the expected combinator decision
 * is known and the assertion is non-vacuous): three {@code execute} branches each return a constant {@code vote} value
 * keyed only by the branch — {@link #BRANCH_A} votes {@link #VOTE_YES yes}, {@link #BRANCH_B} and {@link #BRANCH_C} vote
 * {@link #VOTE_NO no}. The combinator predicate is {@link #VOTED_YES} ("this branch's committed result voted
 * {@code yes}"). Over {A=yes, B=no, C=no}, the documented semantics fix every decision:
 * <ul>
 *   <li><strong>{@code anyMatch}</strong> (race / first-match-wins): proceeds on the FIRST branch in declaration order
 *       whose committed outcome satisfies the predicate; result is "matched" iff ≥1 branch satisfied it. Here exactly
 *       one branch (A) votes yes, so the decision is <strong>matched</strong> and the winner is unambiguously A
 *       (a single matching branch makes the winner identity timing-independent);</li>
 *   <li><strong>{@code allMatch}</strong> (guard / all-or-first-fail): result is "matched" iff ALL branches satisfied
 *       the predicate; short-circuits on the first non-matching branch. Here B and C voted no, so the decision is
 *       <strong>unmatched</strong>;</li>
 *   <li><strong>{@code noneMatch}</strong> (fail-fast): result is "matched" iff NO branch satisfied the predicate. Here
 *       A voted yes, so a match exists and the decision is <strong>unmatched</strong>.</li>
 * </ul>
 * The decisions are made observable per {@code workflowId} two ways the committed log captures: (1) a payload key per
 * combinator written via {@code execute} + {@link CombineGlobalAndLocalPayloadReducer#INSTANCE} so it lands in the
 * engine's reconstructed payload ({@link #KEY_ANY_MATCHED}/{@link #KEY_ALL_MATCHED}/{@link #KEY_NONE_MATCHED}, plus the
 * {@code anyMatch} winner's step name under {@link #KEY_ANY_WINNER}); and (2) a <em>distinct</em> post-combinator
 * {@code execute} step name encoding each combinator's decision ({@code anyMatchMatched} vs {@code anyMatchUnmatched},
 * etc.). The body is a pure {@code execute} sequence with no external wait, so the instance reaches a terminal
 * (COMPLETED) status on its own — keeping the harness's liveness assertion simple while still driving the combinator
 * decision through the crash/restart/reorder faults, exactly where a replay-stability bug (a combinator resolving a
 * different decision after replay) would show.
 * <p>
 * Determinism (axon-flow-workflow skill §3.3): each branch returns a constant keyed only by its step name (no
 * wall-clock/random/external state), so the branches' committed terminal outcomes — and therefore every combinator
 * decision — are a pure function of the history, and replaying it resolves the identical decisions (the replay-stability
 * facet INV-14 shares with INV-4 {@code DeterministicReplay}). The branches are driven to their terminal outcome in
 * declaration order (each {@code execute} handle is awaited before the next is launched) rather than left genuinely
 * overlapping in-flight on purpose: three concurrent {@code execute} branches commit their {@code STARTED}/
 * {@code COMPLETED} events in non-deterministic relative order on the body executor's separate threads (the F-2 surface,
 * now intra-instance), which would make the instance's committed subsequence vary across runs and break INV-4. Driving
 * them in a fixed order makes the committed log a stable function of the history while still exercising each
 * combinator's full decision logic (short-circuit, {@code matched()}/{@code unmatched()}) over three real terminal
 * {@code WorkflowStepResult} handles — and the combinator decision is a pure function of the branches' terminal
 * outcomes, independent of whether they ran concurrently. (The genuinely-overlapping in-flight case is covered by the
 * engine's own example suite: {@code AnyRaceWorkflow} / {@code AllMatchGuardWorkflow} / {@code NoneMatchGuardWorkflow}.)
 * {@code matched()}/{@code unmatched()} are read only after {@code .await()} (axon-flow-workflow skill §4: a
 * combinator's categorized lists must only be read once it has resolved). The {@code execute} bodies also bump the
 * shared {@link CountingEffects} counter like the other simulation workflows; INV-14 itself reads only the committed
 * branch outcomes + the recorded combinator decision, never the effect counters.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class CombinatorWorkflow {

    /**
     * Logical workflow name (single definition).
     */
    public static final String WORKFLOW_NAME = "CombinatorWorkflow";

    /**
     * Payload key each branch writes its vote under.
     */
    public static final String KEY_VOTE = "vote";

    /**
     * The branch vote value the combinator predicate {@link #VOTED_YES} matches on.
     */
    public static final String VOTE_YES = "yes";

    /**
     * The branch vote value the predicate does not match.
     */
    public static final String VOTE_NO = "no";

    /**
     * First parallel branch — votes {@link #VOTE_YES yes} (the single matching branch, so it is the {@code anyMatch}
     * winner).
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
     * Post-{@code anyMatch} step name when the decision is "matched" (the expected outcome here: ≥1 branch voted yes).
     */
    public static final String STEP_ANY_MATCHED = "anyMatchMatched";

    /**
     * Post-{@code anyMatch} step name when the decision is "unmatched" (no branch voted yes — must NOT occur here).
     */
    public static final String STEP_ANY_UNMATCHED = "anyMatchUnmatched";

    /**
     * Post-{@code allMatch} step name when the decision is "matched" (all branches voted yes — must NOT occur here).
     */
    public static final String STEP_ALL_MATCHED = "allMatchMatched";

    /**
     * Post-{@code allMatch} step name when the decision is "unmatched" (the expected outcome here: not all voted yes).
     */
    public static final String STEP_ALL_UNMATCHED = "allMatchUnmatched";

    /**
     * Post-{@code noneMatch} step name when the decision is "matched" (no branch voted yes — must NOT occur here).
     */
    public static final String STEP_NONE_MATCHED = "noneMatchMatched";

    /**
     * Post-{@code noneMatch} step name when the decision is "unmatched" (the expected outcome here: a branch voted yes).
     */
    public static final String STEP_NONE_UNMATCHED = "noneMatchUnmatched";

    /**
     * Payload key recording the {@code anyMatch} decision ({@code true} = matched).
     */
    public static final String KEY_ANY_MATCHED = "anyMatched";

    /**
     * Payload key recording the {@code allMatch} decision ({@code true} = matched).
     */
    public static final String KEY_ALL_MATCHED = "allMatched";

    /**
     * Payload key recording the {@code noneMatch} decision ({@code true} = matched).
     */
    public static final String KEY_NONE_MATCHED = "noneMatched";

    /**
     * Payload key recording which branch step won the {@code anyMatch} race (the first matching branch in declaration
     * order — here unambiguously {@link #BRANCH_A}).
     */
    public static final String KEY_ANY_WINNER = "anyWinner";

    /**
     * The combinator predicate: a branch result whose committed payload voted {@link #VOTE_YES yes}. A pure function of
     * the branch's committed result map — the same handle resolves the same way on replay.
     */
    public static final Predicate<WorkflowStepResult> VOTED_YES =
            r -> r.result().map(m -> VOTE_YES.equals(m.get(KEY_VOTE))).orElse(false);

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public CombinatorWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: launch three parallel branch {@code execute} steps (all in-flight together), then fold them through
     * {@code anyMatch}, {@code allMatch} and {@code noneMatch} over the {@link #VOTED_YES} predicate. Each combinator's
     * decision is recorded both as a payload key (merged into the payload) and as a distinct post-combinator step name,
     * so the committed log makes the decision observable per {@code workflowId}. Completes on its own (execute-only).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        // Three branch steps, each driven to its terminal outcome in declaration order, then all three completed handles
        // are folded through the combinators below. Each returns a constant vote keyed only by the branch
        // (deterministic), so the committed branch outcomes — and therefore every combinator decision below — are a pure
        // function of the history. The branches are awaited in declaration order rather than left genuinely in-flight on
        // purpose: three concurrent execute branches commit their STARTED/COMPLETED events in non-deterministic relative
        // order on the body executor's separate threads (the F-2 surface, now intra-instance), which would make the
        // instance's committed subsequence vary across runs and break INV-4 (DeterministicReplay). Driving them in a
        // fixed order makes the committed log a stable function of the history while still exercising each combinator's
        // full decision logic (short-circuit, matched()/unmatched()) over three real terminal WorkflowStepResult handles
        // — and the combinator decision is a pure function of the branches' terminal outcomes, independent of whether
        // they ran concurrently. (The genuinely-overlapping in-flight case is covered by the engine's own example suite:
        // AnyRaceWorkflow / AllMatchGuardWorkflow / NoneMatchGuardWorkflow.)
        WorkflowStepResult branchA = vote(ctx, workflowId, BRANCH_A, VOTE_YES);
        WorkflowStepResult branchB = vote(ctx, workflowId, BRANCH_B, VOTE_NO);
        WorkflowStepResult branchC = vote(ctx, workflowId, BRANCH_C, VOTE_NO);

        // anyMatch (race / first-match-wins): matched iff ≥1 branch voted yes; the winner is the first matching branch
        // in declaration order. matched()/unmatched() read only after await() (skill §4).
        CombinatorWorkflowStepResult any = ctx.anyMatch(VOTED_YES, branchA, branchB, branchC);
        any.await();
        boolean anyMatched = any.success();
        String anyWinner = anyMatched && !any.matched().isEmpty()
                ? any.matched().get(0).getStepName()
                : "";

        // allMatch (guard / all-or-first-fail): matched iff ALL branches voted yes; short-circuits on first non-match.
        CombinatorWorkflowStepResult all = ctx.allMatch(VOTED_YES, branchA, branchB, branchC);
        all.await();
        boolean allMatched = all.success();

        // noneMatch (fail-fast): matched iff NO branch voted yes; short-circuits on first match.
        CombinatorWorkflowStepResult none = ctx.noneMatch(VOTED_YES, branchA, branchB, branchC);
        none.await();
        boolean noneMatched = none.success();

        // Record each decision into the payload (combine_local_and_global, so it lands in the engine's reconstructed
        // payload) AND as a distinct post-combinator step name encoding the decision.
        recordDecision(ctx, workflowId, anyMatched ? STEP_ANY_MATCHED : STEP_ANY_UNMATCHED,
                       Map.of(KEY_ANY_MATCHED, anyMatched, KEY_ANY_WINNER, anyWinner));
        recordDecision(ctx, workflowId, allMatched ? STEP_ALL_MATCHED : STEP_ALL_UNMATCHED,
                       Map.of(KEY_ALL_MATCHED, allMatched));
        recordDecision(ctx, workflowId, noneMatched ? STEP_NONE_MATCHED : STEP_NONE_UNMATCHED,
                       Map.of(KEY_NONE_MATCHED, noneMatched));
    }

    /**
     * One branch: an {@code execute} step that bumps the effect counter and returns its constant vote (merged into the
     * payload via {@code CombineGlobalAndLocalPayloadReducer} so the committed result records the vote). Awaited before
     * returning so the branch reaches its terminal outcome (and commits its events) in declaration order — keeping the
     * instance's committed subsequence a stable function of the history (see {@link #execute} for why). The returned
     * handle is already terminal, so the combinators read its {@code matched()}/{@code unmatched()} without blocking.
     */
        private WorkflowStepResult vote(SimpleWorkflowContext ctx, String workflowId,
                                    String branch, String voteValue) {
        WorkflowStepResult result = ctx.execute(
                branch,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, branch);
                    return Map.of(KEY_VOTE, voteValue);
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
        result.await();
        return result;
    }

    /**
     * Records one combinator's decision: a post-combinator {@code execute} step whose distinct name encodes the decision
     * and whose result merges the decision payload keys into the workflow payload (so the engine's reconstructed payload
     * carries them).
     */
    private void recordDecision(SimpleWorkflowContext ctx, String workflowId,
                                String stepName, Map<String, Object> decisionPayload) {
        ctx.awaitExecute(
                stepName,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, stepName);
                    return decisionPayload;
                },
                step -> step.resultPayloadReducer(CombineGlobalAndLocalPayloadReducer.INSTANCE));
    }
}
