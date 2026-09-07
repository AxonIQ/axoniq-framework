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
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;

import java.util.Map;

/**
 * Minimal workflow whose single step <strong>always fails</strong> under a {@code RetryPolicy.maxRetries(k)}, used to
 * exercise INVARIANTS.md INV-8 ({@code RetryBound}) on the retry-exhaustion path in a fully deterministic way.
 * <p>
 * Mirrors the retry-exhaustion case of {@code examples/simple}'s {@code RetryWorkflow} (its {@code retryExhaustion}
 * step): an {@code execute} action that throws on every attempt with a retry policy, awaited via
 * {@code WorkflowStepResult.await()} which blocks until terminal and does <strong>not</strong> propagate the step
 * failure — so the workflow still reaches a terminal COMPLETED status (axon-flow-workflow skill §4: {@code execute}'s
 * blocking variants do not fail the workflow on a step failure).
 * <p>
 * Because the action always throws, the engine records exactly the bound's worth of attempts: one {@code STARTED} (the
 * first attempt) followed by {@code RETRYING ×k} (one per retry) and then the terminal {@code FAILED} — i.e. exactly
 * {@code k + 1} attempt records (STARTED/RETRYING). This is the tight, deterministic case the dedicated
 * {@code Inv8RetryBoundScenario} asserts against: attempt records {@code == k + 1 ≤ maxRetries + 1}. No backoff is
 * configured, so the retry sequence does not depend on virtual-time advancement, keeping the scenario robust.
 * <p>
 * The step name ({@link #STEP_FLAKY}) and a preceding {@code reserveInventory} (which always succeeds) give a real
 * step record before the flaky one; the workflow name differs from {@link OrderWorkflow}/{@link CancellingWorkflow} so
 * the definitions never collide. Wired through the same {@code EngineInstance.WorkflowRegistration} mechanism INV-7
 * generalized, so it reuses all the crash/recover infrastructure.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class RetryingWorkflow {

    /**
     * Logical workflow name, stable and distinct from {@link OrderWorkflow#WORKFLOW_NAME} and
     * {@link CancellingWorkflow#WORKFLOW_NAME}.
     */
    public static final String WORKFLOW_NAME = "RetryingWorkflow";

    /**
     * A step that always succeeds, recorded before the flaky step so there is a non-flaky record in the history.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * The always-failing step carrying the retry policy — the one INV-8 ({@code RetryBound}) bounds.
     */
    public static final String STEP_FLAKY = "flakyShip";

    /**
     * The {@code maxRetries} configured on {@link #STEP_FLAKY}. The engine must record at most this many {@code RETRYING}
     * events plus the single {@code STARTED} — at most {@code FLAKY_MAX_RETRIES + 1} attempt records.
     */
    public static final int FLAKY_MAX_RETRIES = 3;

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the {@code execute} bodies bump a counter here.
     */
    public RetryingWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body: record one successful step, then drive an always-failing step under a retry policy to exhaustion.
     * Re-run from the start on every (re)execution; on replay the recorded {@code reserveInventory} returns its cached
     * result and the flaky step resumes from its persisted RETRYING/terminal state.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        ctx.awaitExecute(
                STEP_RESERVE_INVENTORY,
                Map.of(),
                (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY))
        );

        // Always-failing step with maxRetries(k): STARTED + RETRYING×k + FAILED = exactly k+1 attempt records. Awaited
        // via execute(...).await() (not awaitExecute) so retry exhaustion ends the STEP as FAILED without failing the
        // WORKFLOW — it goes on to COMPLETED.
        WorkflowStepResult flaky = ctx.execute(
                STEP_FLAKY,
                Map.of(),
                (pc, payload) -> {
                    int attempt = effects.record(workflowId, STEP_FLAKY);
                    throw new IllegalStateException("flakyShip always fails (attempt " + attempt + ")");
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(FLAKY_MAX_RETRIES))
        );
        flaky.await();
    }
}
