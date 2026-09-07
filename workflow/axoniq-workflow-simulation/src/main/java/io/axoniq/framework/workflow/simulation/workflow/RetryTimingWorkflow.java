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
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;

import java.time.Duration;
import java.util.Map;

/**
 * Workflow that drives the retry/timeout <em>edges</em> INV-8 ({@code RetryBound}) and INV-9 ({@code TimeoutsFire})
 * deliberately did not cover, used to exercise INVARIANTS.md INV-21 ({@code RetryTimingAndExhaustionEdges}) in a fully
 * deterministic way (every failure/timeout decision is driven by the crash-surviving {@link CountingEffects} attempt
 * counter — like {@link OrderWorkflow#STEP_SHIP_ORDER} — and by the step payload, never by randomness).
 * <p>
 * The body has two halves, run by two separate entry points so the per-attempt {@code execute}-timeout edge (which rides
 * the non-injectable {@code orTimeout} residual — ARCHITECTURE.md §12 / adoc D5, exactly like INV-9's wait timeout) can
 * be driven in isolation from the deterministic retry/backoff edges:
 * <ul>
 *   <li>{@link #executeRetryEdges(SimpleWorkflowContext)} (the {@code retryedge-} workhorse) — the three backoff
 *       strategies + the {@code retryWhile} predicate + the {@code onRetry} re-fire probe:
 *     <ol>
 *       <li>{@link #STEP_RESERVE} ({@code execute}) — one always-succeeding step, so there is a real record before the
 *           retrying steps;</li>
 *       <li>{@link #STEP_FIXED} ({@code execute}, {@code maxRetries(2)} + {@code BackoffStrategy.fixed(}{@link #FIXED_DELAY}{@code )}
 *           + an {@code onRetry} that bumps {@link OnRetryFires}) — fails on attempts 1..2, succeeds on attempt 3, so the
 *           engine records {@code STARTED + RETRYING×2 + COMPLETED}; each {@code RETRYING} is scheduled
 *           {@link #FIXED_DELAY} after the previous attempt's recorded timestamp, so the recorded gaps are constant;</li>
 *       <li>{@link #STEP_LINEAR} ({@code linear(}{@link #LINEAR_BASE}{@code )}) — same fail-twice-then-succeed shape; the
 *           backoff before retry attempt {@code a} is {@code LINEAR_BASE × a} (growing);</li>
 *       <li>{@link #STEP_EXPONENTIAL} ({@code exponential(}{@link #EXP_BASE}{@code , }{@link #EXP_MAX}{@code )}) — same
 *           shape; the backoff before retry attempt {@code a} is {@code min(EXP_BASE × 2^(a-1), EXP_MAX)} (doubling);</li>
 *       <li>{@link #STEP_RETRY_WHILE} ({@code maxRetries(5)} + {@code retryWhile(ctx -> ctx.attempt() <}
 *           {@link #RETRY_WHILE_STOP_ATTEMPT}{@code )}, always failing) — the predicate stops retrying <em>before</em>
 *           {@code maxRetries} is reached, so the step records {@code STARTED + RETRYING×(RETRY_WHILE_STOP_ATTEMPT-1) + FAILED}
 *           — strictly fewer attempt records than {@code maxRetries + 1}, the bound INV-8 alone would allow.</li>
 *     </ol>
 *     The step results are awaited via the non-blocking {@code execute(...).await()} (not {@code awaitExecute}), so a
 *     step that exhausts to {@code FAILED} ends the STEP without failing the WORKFLOW (the workflow still reaches
 *     COMPLETED — see {@link RetryingWorkflow}), keeping the instance terminal for the harness liveness check.</li>
 *   <li>{@link #executeTimeoutEdge(SimpleWorkflowContext)} (the {@code retrytimeout-} entry point) — the per-attempt
 *       {@code execute} timeout (the D5 residual INV-9 left to the wait path):
 *     <ol>
 *       <li>{@link #STEP_RESERVE} ({@code execute}) — the same always-succeeding pre-step;</li>
 *       <li>{@link #STEP_SLOW_EXECUTE} ({@code execute}, {@code timeout(}{@link #EXECUTE_TIMEOUT}{@code )} +
 *           {@code maxRetries(}{@link #SLOW_EXECUTE_MAX_RETRIES}{@code )}) — an action that blocks on a never-completing
 *           latch, so the per-attempt {@code orTimeout} window elapses and the engine records a {@code TIMED_OUT}
 *           outcome for each attempt; with the retry policy the total budget is {@code (retries + 1) × timeout} and the
 *           step ultimately reaches a terminal {@code TIMED_OUT}, never hanging.</li>
 *     </ol>
 *     Awaited via {@code execute(...).await()} so the timed-out step ends the STEP terminally without failing the
 *     WORKFLOW (it still reaches COMPLETED).</li>
 * </ul>
 * The workflow name differs from the other simulation workflows so the definitions never collide; both entry points are
 * registered as their own {@code EngineInstance} factory, reusing the {@code WorkflowRegistration} mechanism INV-7
 * generalized, so they inherit all the crash/recover infrastructure.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class RetryTimingWorkflow {

    /**
     * Logical workflow name for the retry-edges entry point, stable and distinct from the other simulation workflows.
     */
    public static final String RETRY_EDGES_WORKFLOW_NAME = "RetryTimingRetryEdgesWorkflow";

    /**
     * Logical workflow name for the per-attempt-execute-timeout entry point, stable and distinct.
     */
    public static final String TIMEOUT_EDGE_WORKFLOW_NAME = "RetryTimingTimeoutEdgeWorkflow";

    /**
     * A step that always succeeds, recorded before the retrying/timing-out steps so there is a non-flaky record first.
     */
    public static final String STEP_RESERVE = "reserveSlot";

    /**
     * The {@code BackoffStrategy.fixed} step: fails twice then succeeds under {@code maxRetries(2)}.
     */
    public static final String STEP_FIXED = "fixedBackoffCall";

    /**
     * The {@code BackoffStrategy.linear} step: fails twice then succeeds under {@code maxRetries(2)}.
     */
    public static final String STEP_LINEAR = "linearBackoffCall";

    /**
     * The {@code BackoffStrategy.exponential} step: fails twice then succeeds under {@code maxRetries(2)}.
     */
    public static final String STEP_EXPONENTIAL = "exponentialBackoffCall";

    /**
     * The {@code retryWhile}-bounded step: always fails under {@code maxRetries(5)} but the predicate stops it early.
     */
    public static final String STEP_RETRY_WHILE = "retryWhileCall";

    /**
     * The per-attempt-{@code execute}-timeout step: its action blocks past its per-attempt {@code timeout}.
     */
    public static final String STEP_SLOW_EXECUTE = "slowExecuteCall";

    /**
     * The {@code maxRetries} configured on the three backoff steps. Each fails on attempts {@code 1..this} and succeeds on
     * attempt {@code this + 1}, so the engine records exactly one {@code STARTED} + {@code this} {@code RETRYING} records.
     */
    public static final int BACKOFF_MAX_RETRIES = 2;

    /**
     * The {@code maxRetries} configured on {@link #STEP_RETRY_WHILE}. Deliberately larger than the number of retries the
     * {@code retryWhile} predicate permits, so the predicate (not the bound) is what stops retrying.
     */
    public static final int RETRY_WHILE_MAX_RETRIES = 5;

    /**
     * The 1-based retry attempt at which {@link #STEP_RETRY_WHILE}'s {@code retryWhile} predicate stops retrying: the
     * predicate is {@code ctx.attempt() < RETRY_WHILE_STOP_ATTEMPT}, so retry attempt {@code RETRY_WHILE_STOP_ATTEMPT}
     * is refused and the step goes terminal. With the predicate stopping after attempt {@code RETRY_WHILE_STOP_ATTEMPT-1}
     * the step records {@code STARTED + RETRYING×(RETRY_WHILE_STOP_ATTEMPT-1) + FAILED}.
     */
    public static final int RETRY_WHILE_STOP_ATTEMPT = 2;

    /**
     * The {@code maxRetries} configured on {@link #STEP_SLOW_EXECUTE}. With a per-attempt {@link #EXECUTE_TIMEOUT} the
     * total budget before the step goes terminal is {@code (SLOW_EXECUTE_MAX_RETRIES + 1) × EXECUTE_TIMEOUT}.
     */
    public static final int SLOW_EXECUTE_MAX_RETRIES = 1;

    /**
     * Fixed backoff delay before every retry of {@link #STEP_FIXED}.
     */
    public static final Duration FIXED_DELAY = Duration.ofMillis(200);

    /**
     * Linear backoff base for {@link #STEP_LINEAR}: the delay before retry attempt {@code a} is {@code LINEAR_BASE × a}.
     */
    public static final Duration LINEAR_BASE = Duration.ofMillis(100);

    /**
     * Exponential backoff base for {@link #STEP_EXPONENTIAL}: the delay before retry attempt {@code a} is
     * {@code min(EXP_BASE × 2^(a-1), EXP_MAX)}.
     */
    public static final Duration EXP_BASE = Duration.ofMillis(100);

    /**
     * Exponential backoff cap for {@link #STEP_EXPONENTIAL}.
     */
    public static final Duration EXP_MAX = Duration.ofSeconds(10);

    /**
     * Per-attempt {@code execute} timeout for {@link #STEP_SLOW_EXECUTE}.
     */
    public static final Duration EXECUTE_TIMEOUT = Duration.ofSeconds(5);

    private final CountingEffects effects;
    private final OnRetryFires onRetryFires;
    private final java.util.concurrent.CountDownLatch slowExecuteLatch;

    /**
     * Creates the workflow bound to the given registries and a never-counted-down latch the slow {@code execute} blocks
     * on (so the per-attempt {@code orTimeout} fires deterministically).
     *
     * @param effects         registry that survives crashes; the {@code execute} bodies bump an attempt counter here.
     * @param onRetryFires    registry that survives crashes; the {@code onRetry} handlers bump a fire counter here.
     * @param slowExecuteLatch a latch {@link #STEP_SLOW_EXECUTE}'s action blocks on indefinitely (never counted down), so
     *                        its per-attempt {@code timeout} window elapses and the engine records {@code TIMED_OUT}.
     */
    public RetryTimingWorkflow(CountingEffects effects, OnRetryFires onRetryFires,
                               java.util.concurrent.CountDownLatch slowExecuteLatch) {
        this.effects = effects;
        this.onRetryFires = onRetryFires;
        this.slowExecuteLatch = slowExecuteLatch;
    }

    /**
     * The retry-edges body: one success step, then three backoff-strategy steps (fixed/linear/exponential) that each
     * fail twice then succeed, then one {@code retryWhile}-bounded always-failing step. Re-run from the start on every
     * (re)execution; on replay each recorded-terminal step returns its cached result and emits nothing, and the engine
     * resumes a {@code RETRYING} step from its persisted attempt without re-invoking {@code onRetry}.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeRetryEdges(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        ctx.awaitExecute(
                STEP_RESERVE,
                Map.of(),
                (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE))
        );

        WorkflowStepResult fixed = ctx.execute(
                STEP_FIXED,
                Map.of(),
                (pc, payload) -> failTwiceThenSucceed(workflowId, STEP_FIXED),
                step -> step.retryPolicy(RetryPolicy.maxRetries(BACKOFF_MAX_RETRIES)
                                                    .withBackoff(BackoffStrategy.fixed(FIXED_DELAY))
                                                    .onRetry(rc -> onRetryFires.record(workflowId, STEP_FIXED)))
        );
        fixed.await();

        WorkflowStepResult linear = ctx.execute(
                STEP_LINEAR,
                Map.of(),
                (pc, payload) -> failTwiceThenSucceed(workflowId, STEP_LINEAR),
                step -> step.retryPolicy(RetryPolicy.maxRetries(BACKOFF_MAX_RETRIES)
                                                    .withBackoff(BackoffStrategy.linear(LINEAR_BASE))
                                                    .onRetry(rc -> onRetryFires.record(workflowId, STEP_LINEAR)))
        );
        linear.await();

        WorkflowStepResult exponential = ctx.execute(
                STEP_EXPONENTIAL,
                Map.of(),
                (pc, payload) -> failTwiceThenSucceed(workflowId, STEP_EXPONENTIAL),
                step -> step.retryPolicy(RetryPolicy.maxRetries(BACKOFF_MAX_RETRIES)
                                                    .withBackoff(BackoffStrategy.exponential(EXP_BASE, EXP_MAX))
                                                    .onRetry(rc -> onRetryFires.record(workflowId, STEP_EXPONENTIAL)))
        );
        exponential.await();

        // retryWhile stops retrying when the predicate says so (attempt RETRY_WHILE_STOP_ATTEMPT is refused), BEFORE
        // RETRY_WHILE_MAX_RETRIES is reached — so the step records strictly fewer attempts than maxRetries+1 would allow.
        WorkflowStepResult retryWhile = ctx.execute(
                STEP_RETRY_WHILE,
                Map.of(),
                (pc, payload) -> {
                    int attempt = effects.record(workflowId, STEP_RETRY_WHILE);
                    throw new IllegalStateException("retryWhileCall always fails (attempt " + attempt + ")");
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(RETRY_WHILE_MAX_RETRIES)
                                                    .retryWhile(rc -> rc.attempt() < RETRY_WHILE_STOP_ATTEMPT)
                                                    .onRetry(rc -> onRetryFires.record(workflowId, STEP_RETRY_WHILE)))
        );
        retryWhile.await();
    }

    /**
     * The per-attempt-execute-timeout body: one success step, then a slow {@code execute} whose action blocks past its
     * per-attempt {@code timeout} so the engine records a {@code TIMED_OUT} outcome (under {@code maxRetries} the step
     * reaches a terminal {@code TIMED_OUT} once the total {@code (retries+1)×timeout} budget elapses). Re-run from the
     * start on every (re)execution.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeTimeoutEdge(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        // A long (effectively-infinite) per-attempt timeout on the pre-step so it does NOT time out when the scenario
        // pre-advances the virtual clock far ahead to make the SLOW step's short per-attempt window already elapsed (the
        // clean line-160 immediate-fire path of ExecuteDelegate). reserveSlot completes instantly, so the long timeout is
        // never approached; only STEP_SLOW_EXECUTE carries the short EXECUTE_TIMEOUT the scenario drives past.
        ctx.awaitExecute(
                STEP_RESERVE,
                Map.of(),
                (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE)),
                step -> step.timeout(Duration.ofDays(365))
        );

        WorkflowStepResult slow = ctx.execute(
                STEP_SLOW_EXECUTE,
                Map.of(),
                (pc, payload) -> {
                    effects.record(workflowId, STEP_SLOW_EXECUTE);
                    // Block indefinitely: the per-attempt orTimeout window (EXECUTE_TIMEOUT) elapses while this action is
                    // still running, so the engine records a TIMED_OUT outcome for the attempt. The latch is never
                    // counted down, so the only way the attempt resolves is the timeout (or the engine interrupting the
                    // action thread on shutdown, which surfaces as an InterruptedException re-thrown to the runtime).
                    try {
                        slowExecuteLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("slowExecuteCall interrupted while blocked past its timeout", e);
                    }
                    return Map.of("done", true);
                },
                step -> step.timeout(EXECUTE_TIMEOUT)
                            .retryPolicy(RetryPolicy.maxRetries(SLOW_EXECUTE_MAX_RETRIES))
        );
        slow.await();
    }

    /**
     * Fails on attempts {@code 1..BACKOFF_MAX_RETRIES} (driven by the crash-surviving {@link CountingEffects} counter, so
     * the failure sequence is reproducible across a crash) and succeeds on attempt {@code BACKOFF_MAX_RETRIES + 1} — the
     * genuine-retry shape that records {@code STARTED + RETRYING×BACKOFF_MAX_RETRIES + COMPLETED}.
     */
        private Map<String, Object> failTwiceThenSucceed(String workflowId, String stepName) {
        int attempt = effects.record(workflowId, stepName);
        if (attempt <= BACKOFF_MAX_RETRIES) {
            throw new IllegalStateException(stepName + " transient failure on attempt " + attempt + " (forces a retry)");
        }
        return Map.of("succeededOnAttempt", attempt);
    }
}
