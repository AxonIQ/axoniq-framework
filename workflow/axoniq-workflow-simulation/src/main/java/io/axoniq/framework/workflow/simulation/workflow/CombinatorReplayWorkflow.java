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
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BranchASignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BranchBSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BranchCSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.GateOpenedEvent;

import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Combinator <strong>replay-determinism</strong> probes: does what the body OBSERVES from a combinator —
 * {@code matched()}, {@code unmatched()}, the {@code anyMatch} winner, the {@code noneMatch} verdict — recompute the
 * SAME way when the body re-runs after a crash? Combinators are not durable steps: their categorization is computed
 * lazily, per body run, from the branches' CURRENT states ({@code CombinatorSupport.computeCategories}), so a branch
 * that completes AFTER the live short-circuit but BEFORE a crash is re-categorized on the recovered re-run.
 * <p>
 * Each run pass records what it saw into body-level {@link CountingEffects} counters (prefixed snapshot keys) — a
 * counter present once-per-run on both passes is replay-stable; a counter present on only one pass is the divergence
 * observable. The body then parks on a {@code gate} wait so the scenario controls the crash window, and completes
 * when the gate opens.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class CombinatorReplayWorkflow {

    /**
     * Logical workflow name.
     */
    public static final String WORKFLOW_NAME = "CombinatorReplayWorkflow";

    /**
     * Probe: {@code allMatch} short-circuits on a failed branch while a third branch is still in flight; the third
     * branch completes BEFORE the crash; the recovered re-run recomputes the categories.
     */
    public static final String MODE_ALL_MATCH_LATE = "allMatchLate";

    /**
     * Probe: {@code anyMatch} winner identity across replay — branch B completes first live; after the crash BOTH
     * branches are cached COMPLETED and the winner is recomputed from the event-sourced first-completed order.
     */
    public static final String MODE_ANY_MATCH_WINNER = "anyMatchWinner";

    /**
     * Probe: {@code noneMatch(failure)} verdict with a TIMED_OUT branch (the C-2 foot-gun: {@code failure()} is true
     * only for FAILED, so a timed-out branch passes the guard) + the verdict's replay stability.
     */
    public static final String MODE_NONE_MATCH_TIMEOUT = "noneMatchTimeout";

    /**
     * Branch step names.
     */
    public static final String STEP_BRANCH_A = "branchA";

    /**
     * Branch B.
     */
    public static final String STEP_BRANCH_B = "branchB";

    /**
     * Branch C (the late completer in the allMatch probe).
     */
    public static final String STEP_BRANCH_C = "branchC";

    /**
     * The post-snapshot gate wait that keeps the instance parked across the scenario's crash window.
     */
    public static final String STEP_GATE = "awaitGate";

    /**
     * The timed branch's wait window in the {@code noneMatch} probe (elapsed deterministically by the scenario via
     * the era-anchored advance).
     */
    public static final java.time.Duration NONE_MATCH_WAIT_TIMEOUT = java.time.Duration.ofSeconds(10);

    /**
     * Effect-counter key counting body run passes (1 = live only, 2 = live + one recovered re-run).
     */
    public static final String EFFECT_RUN = "combinatorRun";

    /**
     * Effect-counter key prefix for a branch observed in {@code matched()} on a run pass.
     */
    public static final String EFFECT_MATCHED_PREFIX = "matched:";

    /**
     * Effect-counter key prefix for a branch observed in {@code unmatched()} on a run pass.
     */
    public static final String EFFECT_UNMATCHED_PREFIX = "unmatched:";

    /**
     * Effect-counter key prefix for the combinator's verdict ({@code success()} value) on a run pass.
     */
    public static final String EFFECT_VERDICT_PREFIX = "verdict:";

    /**
     * Effect-counter key prefix for the {@code anyMatch} winner's step name on a run pass.
     */
    public static final String EFFECT_WINNER_PREFIX = "winner:";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the per-run snapshot counters land here.
     */
    public CombinatorReplayWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body; the {@code mode} payload field selects the probe deterministically.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");
        Object mode = ctx.workflowPayload().get("mode");

        effects.record(workflowId, EFFECT_RUN);

        switch (String.valueOf(mode)) {
            case MODE_ALL_MATCH_LATE -> allMatchLate(ctx, workflowId, orderId);
            case MODE_ANY_MATCH_WINNER -> anyMatchWinner(ctx, workflowId, orderId);
            case MODE_NONE_MATCH_TIMEOUT -> noneMatchTimeout(ctx, workflowId, orderId);
            default -> throw new IllegalStateException("unknown combinator probe mode: " + mode);
        }

        // Park across the scenario's crash window; the gate release lets the instance complete.
        ctx.awaitEvent(STEP_GATE, GateOpenedEvent.class,
                       associate(payloadProperty("orderId"), equalsTo(orderId)),
                       step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));
    }

    private void allMatchLate(SimpleWorkflowContext ctx, String workflowId,
                              Object orderId) {
        // A completes immediately; B fails immediately (terminal non-match, forcing the allMatch short-circuit);
        // C is a wait the scenario completes LATE — after the live short-circuit, before the crash.
        var a = ctx.execute(STEP_BRANCH_A, Map.of(),
                            (pc, p) -> Map.of("a", true),
                            step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));
        var b = ctx.execute(STEP_BRANCH_B, Map.of(),
                            (pc, p) -> {
                                throw new IllegalStateException("branchB always fails");
                            },
                            step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));
        var c = ctx.waitForEvent(STEP_BRANCH_C, BranchCSignalEvent.class,
                                 associate(payloadProperty("orderId"), equalsTo(orderId)),
                                 step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));

        var guard = ctx.allMatch(WorkflowStepResult::success, a, b, c);
        guard.await();
        snapshot(workflowId, guard.success(), guard.matched(), guard.unmatched());
    }

    private void anyMatchWinner(SimpleWorkflowContext ctx, String workflowId,
                                Object orderId) {
        // Both branches are waits; the scenario delivers B's signal FIRST (the live winner), then A's, so on the
        // recovered re-run BOTH are cached COMPLETED and the winner is recomputed from durable history.
        var a = ctx.waitForEvent(STEP_BRANCH_A, BranchASignalEvent.class,
                                 associate(payloadProperty("orderId"), equalsTo(orderId)),
                                 step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));
        var b = ctx.waitForEvent(STEP_BRANCH_B, BranchBSignalEvent.class,
                                 associate(payloadProperty("orderId"), equalsTo(orderId)),
                                 step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));

        var race = ctx.anyMatch(WorkflowStepResult::success, a, b);
        race.await();
        // The combinator's getStepName() is the synthetic "anyMatch(...)" label, so the winner's IDENTITY is read
        // through matched()'s first element — the winner resolution (findFirstByPredicate) and the matched() ordering
        // (sortByEventSourcedTimestamp) both derive from the same durable first-completed order.
        var matched = race.matched();
        effects.record(workflowId, EFFECT_WINNER_PREFIX
                + (matched.isEmpty() ? "none" : matched.get(0).getStepName()));
        snapshot(workflowId, race.success(), matched, race.unmatched());
    }

    private void noneMatchTimeout(SimpleWorkflowContext ctx, String workflowId,
                                  Object orderId) {
        // A completes; T times out (the scenario elapses its window) — noneMatch(failure) must then decide over a
        // COMPLETED + TIMED_OUT pair: the C-2 foot-gun expects success()==true (TIMED_OUT is not failure()).
        var a = ctx.execute(STEP_BRANCH_A, Map.of(),
                            (pc, p) -> Map.of("a", true),
                            step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT));
        var t = ctx.waitForEvent(STEP_BRANCH_B, BranchBSignalEvent.class,
                                 associate(payloadProperty("orderId"), equalsTo(orderId)),
                                 step -> step.timeout(NONE_MATCH_WAIT_TIMEOUT));

        var guard = ctx.noneMatch(WorkflowStepResult::failure, a, t);
        guard.await();
        snapshot(workflowId, guard.success(), guard.matched(), guard.unmatched());
    }

    private void snapshot(String workflowId, boolean verdict,
                          java.util.List<WorkflowStepResult> matched,
                          java.util.List<WorkflowStepResult> unmatched) {
        effects.record(workflowId, EFFECT_VERDICT_PREFIX + verdict);
        for (WorkflowStepResult r : matched) {
            effects.record(workflowId, EFFECT_MATCHED_PREFIX + r.getStepName());
        }
        for (WorkflowStepResult r : unmatched) {
            effects.record(workflowId, EFFECT_UNMATCHED_PREFIX + r.getStepName());
        }
    }
}
