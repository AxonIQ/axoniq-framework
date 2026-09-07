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
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;

import java.util.Map;

/**
 * Scenario-pinned workflow with a single retried {@code execute} step whose action <strong>succeeds</strong> — used to
 * verify the at-most-once <strong>retry</strong> branch of the F-0 fix (INVARIANTS.md INV-6).
 * <p>
 * The step carries a {@link RetryPolicy}, so when its first attempt is interrupted by a crash in the apply→commit
 * window (the write-then-vanish fault drops its {@code COMPLETED}), the engine must <strong>not</strong> re-run the
 * action in place; it routes the interrupted attempt through the regular error flow, which for a retried step means a
 * {@code RETRYING} transition and a fresh next attempt (not an in-place re-execution of the same attempt). Because the
 * action succeeds, the next attempt then drives the step to {@code COMPLETED} and the workflow to {@code COMPLETED} —
 * exactly the "if there is a retry strategy, go to RETRYING" behaviour. (Contrast {@link RetryingWorkflow}, whose step
 * always throws, so it never reaches a clean {@code STARTED}→action-succeeds window for the write-then-vanish fault to
 * target.)
 * <p>
 * Registered only by its dedicated test (NOT folded into {@code SimulationWorld#defaultRegistrations()}), so the
 * always-on fuzz/smoke instance counts are unperturbed.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class RetryResumeWorkflow {

    /**
     * Logical workflow name, stable and distinct from every other test workflow's name.
     */
    public static final String WORKFLOW_NAME = "RetryResumeWorkflow";

    /**
     * The single retried step whose action succeeds. Its {@code COMPLETED} is the write-then-vanish target.
     */
    public static final String STEP_CHARGE = "chargeWithRetry";

    /**
     * The {@code maxRetries} configured on {@link #STEP_CHARGE}: at least one retry budget so a crash-interrupted first
     * attempt can be re-attempted (RETRYING + next attempt) rather than re-run in place.
     */
    public static final int MAX_RETRIES = 2;

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every action run bumps a counter here.
     */
    public RetryResumeWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body: a single retried, succeeding {@code execute} step. On a clean run the action runs once and the
     * step completes. When the first attempt's {@code COMPLETED} is vanished and the worker crashes, recovery finds the
     * step {@code STARTED} and (at-most-once) does not re-run the action in place; the retry policy turns the interrupted
     * attempt into a {@code RETRYING} transition and a fresh attempt, which then completes the step.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(
                STEP_CHARGE,
                Map.of(),
                (pc, payload) -> Map.of("charged", effects.record(workflowId, STEP_CHARGE)),
                step -> step.retryPolicy(RetryPolicy.maxRetries(MAX_RETRIES))
        );
    }
}
