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
 * A workflow that drives an always-failing {@code execute} step under a LARGE {@code maxRetries} with
 * {@code BackoffStrategy.exponential(...)}, used to pin finding F-9 (S-3) — the {@code BackoffStrategy.exponential}
 * shift/{@code Duration} overflow at large attempt counts, now FIXED (see POC-TLA-DST.adoc and INVARIANTS.md INV-8
 * {@code RetryBound} / INV-21 {@code RetryTimingAndExhaustionEdges}).
 * <p>
 * <strong>Was:</strong> {@code exponential(base, max)} computed
 * {@code factor = 1L << (attempt - 1); computed = base.multipliedBy(factor); return computed > max ? max : computed;} —
 * the cap was applied AFTER the multiply, so for this {@link #BACKOFF_BASE minute-scale base} at
 * {@code attempt == }{@value #OVERFLOW_ATTEMPT} the factor {@code 2^58} made {@code 60s × 2^58} exceed {@code Duration}'s
 * capacity and {@code multipliedBy} threw {@code ArithmeticException} BEFORE the clamp; the {@code delay(attempt)} call in
 * {@code RetryableExecuteDelegate.handleAttemptFailure} (runtime {@code RetryableExecuteDelegate.java:136}) runs on the
 * workflow thread, so the throw propagated into {@code handleWorkflowException}'s {@code default} branch and WEDGED the
 * instance non-terminally (the F-6/S-4 sink).
 * <p>
 * <strong>Now (fixed):</strong> {@code exponential} clamps to {@code max} before any overflowing/negative/wrapped shift or
 * multiply, so {@code delay(attempt)} returns the capped {@link #BACKOFF_MAX max} for every large attempt and never throws.
 * The always-failing step (a plain {@code IllegalStateException} per attempt, driven by the crash-surviving
 * {@link CountingEffects} attempt counter so the failure sequence is reproducible) therefore retries to EXHAUSTION on the
 * clamped schedule, records terminal {@code FAILED}, and the workflow reaches a terminal status — no wedge, no hang.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class BackoffOverflowWorkflow {

    /**
     * Logical workflow name, stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "BackoffOverflowWorkflow";

    /**
     * The always-failing {@code execute} step under the large-{@code maxRetries} exponential backoff policy.
     */
    public static final String STEP_OVERFLOW = "exponentialOverflowCall";

    /**
     * The {@code maxRetries} configured on {@link #STEP_OVERFLOW}: large enough that the (formerly-overflowing)
     * {@value #OVERFLOW_ATTEMPT}th attempt is reached well before exhaustion, so the step proves the fixed
     * {@code exponential} sails past it and then retries to exhaustion ({@code maxRetries + 1} attempt records).
     */
    public static final int MAX_RETRIES = 70;

    /**
     * The exponential backoff base: a MINUTE-scale base so {@code base × 2^(attempt-1)} exceeds {@code Duration}'s
     * capacity (a {@code multipliedBy} {@code ArithmeticException}) at {@code attempt == }{@value #OVERFLOW_ATTEMPT} —
     * BEFORE the {@code attempt == 64} negative-shift point (so the FIRST overflow mode reached is the {@code multipliedBy}
     * throw, the wedge trigger) and well within {@link #MAX_RETRIES} (so the overflow, not exhaustion, is what stops the
     * step).
     */
    public static final Duration BACKOFF_BASE = Duration.ofMinutes(1);

    /**
     * The exponential backoff cap, deliberately SMALL: every uncapped {@code base × 2^(attempt-1)} delay (≥ 1 minute from
     * attempt 1) far exceeds it, so the engine schedules every retry at this small cap — keeping the virtual time the
     * scenario must advance to reach the {@value #OVERFLOW_ATTEMPT}th attempt tiny. The cap is irrelevant to the overflow
     * itself: the cap check happens AFTER the (throwing) {@code multipliedBy}, so it is never reached on the overflow
     * attempt.
     */
    public static final Duration BACKOFF_MAX = Duration.ofSeconds(1);

    /**
     * The 1-based retry attempt at which the OLD {@code BackoffStrategy.exponential(BACKOFF_BASE, …)} overflowed
     * {@code Duration}'s capacity and threw (for a 1-minute base, the factor {@code 1L << (attempt-1) = 2^58} made
     * {@code 60s × 2^58} exceed capacity). The FIXED {@code exponential} clamps to {@link #BACKOFF_MAX max} here instead of
     * throwing, so reaching this attempt proves the overflow is gone; the step then retries on to exhaustion.
     */
    public static final int OVERFLOW_ATTEMPT = 59;

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the failing {@code execute} body bumps an attempt counter here.
     */
    public BackoffOverflowWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: one always-failing {@code execute} step under {@code maxRetries(MAX_RETRIES)} with
     * {@code BackoffStrategy.exponential(BACKOFF_BASE, BACKOFF_MAX)}. With the FIXED {@code exponential} the delay clamps
     * to {@code max} at large attempts (no overflow/throw), so the engine retries the step on the clamped schedule until
     * exhaustion, the step records terminal {@code FAILED}, and the workflow reaches a terminal status — the terminal
     * outcome is the observable.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        WorkflowStepResult overflow = ctx.execute(
                STEP_OVERFLOW,
                Map.of(),
                (pc, payload) -> {
                    int attempt = effects.record(workflowId, STEP_OVERFLOW);
                    throw new IllegalStateException(
                            STEP_OVERFLOW + " always fails (attempt " + attempt + ") to force a retry");
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(MAX_RETRIES)
                                                    .withBackoff(BackoffStrategy.exponential(BACKOFF_BASE, BACKOFF_MAX))));
        overflow.await();
    }
}
