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
 * Minimal workflow whose failing step <strong>always throws</strong>; the body then <strong>propagates</strong> that
 * failure to a terminal <strong>FAILED</strong> workflow status via the {@code ctx.fail(cause)} terminal primitive. Used
 * to exercise INVARIANTS.md INV-16 ({@code FailurePropagation}) — that a step failure never silently hangs or completes,
 * but deterministically drives the instance to a FAILED terminus.
 * <p>
 * This is the deliberate contrast with {@link RetryingWorkflow}, whose flaky step is awaited via
 * {@code execute(...).await()} and whose failure is then <em>ignored</em>, so that workflow COMPLETES (it exercises INV-8
 * {@code RetryBound}, the attempt-count bound — record-level). {@code FailingWorkflow} instead <em>surfaces</em> the
 * failure: it runs the always-throwing step via {@code execute(...)}, awaits it (so the step records a terminal
 * {@code FAILED} status), inspects {@code WorkflowStepResult.failure()}, and calls {@code ctx.fail(cause)} — the
 * supported terminal primitive (axon-flow-workflow skill §3.5: {@code ctx.fail} publishes the single terminal FAILED
 * workflow-status event and ends the workflow immediately; runtime {@code SimpleWorkflowExecution.java:236-253} /
 * {@code TerminateDelegate}). This mirrors {@code examples/simple}'s {@code FailWorkflow}, which reaches
 * {@code WorkflowStatus.FAILED} via {@code ctx.fail(...)}. (Note the engine does <strong>not</strong> auto-fail on an
 * uncaught {@code StepFailedException} re-thrown from {@code awaitExecute} — {@code handleWorkflowException}'s default
 * branch only logs such an exception, leaving the workflow non-terminal — so a workflow that intends to FAIL on a step
 * failure must propagate it explicitly via {@code ctx.fail}.)
 * <p>
 * Both INV-16 failure paths are covered, selected by the {@code maxRetries} the registration pins; in <em>both</em> the
 * step records a terminal {@code FAILED} status and the body then calls {@code ctx.fail}:
 * <ul>
 *   <li><strong>(a) no-retry uncaught exception</strong> — {@code maxRetries == 0}: the step throws once, has no retry
 *       budget, records a terminal {@code FAILED} step on the first (and only) attempt, and the body fails the workflow
 *       immediately ({@link #FAILING_WORKFLOW_NAME_NO_RETRY});</li>
 *   <li><strong>(b) retry exhaustion</strong> — {@code maxRetries == k > 0}: the step throws on every attempt, records
 *       {@code STARTED + RETRYING×k + FAILED}, exhausts the policy, and the body then fails the workflow
 *       ({@link #FAILING_WORKFLOW_NAME_RETRY_EXHAUSTION}). The FAILED <em>propagation</em> this asserts is distinct from
 *       INV-8's attempt-record <em>bound</em> on the same shape.</li>
 * </ul>
 * The failure is fully <strong>deterministic</strong> — the failing step's action throws unconditionally (driven by the
 * step being reached, not by randomness) — so the FAILED terminus is guaranteed and the INV-16 assertion is
 * non-vacuous. No backoff is configured, so the retry sequence does not depend on virtual-time advancement, keeping the
 * scenario robust. Because the workflow reaches a terminal FAILED status on its own, it self-completes (liveness stays
 * simple — a FAILED status is terminal, so the harness's {@code EventuallyTerminates} check is satisfied).
 * <p>
 * A preceding {@code reserveInventory} (which always succeeds) gives a real recorded step before the failing one, so
 * "no step after the failing one begins" is non-vacuous. The step names differ from {@link OrderWorkflow}'s and the
 * workflow names differ from every other test workflow so the definitions never collide. Wired through the same
 * {@code EngineInstance.WorkflowRegistration} mechanism the other DST workflows use, reusing all the crash/recover
 * infrastructure.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class FailingWorkflow {

    /**
     * Logical workflow name for the <strong>no-retry</strong> path (path a): the failing step throws once with no retry
     * budget and the workflow goes FAILED immediately. Stable and distinct from every other test workflow's name.
     */
    public static final String FAILING_WORKFLOW_NAME_NO_RETRY = "FailingWorkflowNoRetry";

    /**
     * Logical workflow name for the <strong>retry-exhaustion</strong> path (path b): the failing step throws on every
     * attempt, exhausts its retry policy, and the workflow then goes FAILED. Stable and distinct from every other test
     * workflow's name (in particular from {@link RetryingWorkflow#WORKFLOW_NAME}, which absorbs its failure and
     * COMPLETES).
     */
    public static final String FAILING_WORKFLOW_NAME_RETRY_EXHAUSTION = "FailingWorkflowRetryExhaustion";

    /**
     * A step that always succeeds, recorded before the failing step so there is a non-failing record in the history and
     * "no step after the failing one begins" is non-vacuous.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * The always-failing step. It records a terminal {@code FAILED} step status; the body then calls
     * {@code ctx.fail(cause)} to drive the workflow to a terminal FAILED status — the property INV-16
     * ({@code FailurePropagation}) asserts.
     */
    public static final String STEP_FAILING = "failingStep";

    /**
     * A step that would run <em>after</em> the failing step if termination were not final. The failing step's exception
     * propagates and ends the workflow, so a correct engine never begins this step — exactly what INV-16 (jointly with
     * INV-7 {@code TerminalIsFinal}) asserts. Its presence in the committed log would be a break.
     */
    public static final String STEP_AFTER_FAILURE = "afterFailure";

    private final CountingEffects effects;
    private final int maxRetries;

    /**
     * Creates the workflow bound to the given effect registry and retry budget.
     *
     * @param effects    registry that survives crashes; the {@code execute} bodies bump a counter here.
     * @param maxRetries the {@code maxRetries} the failing step carries: {@code 0} drives the no-retry immediate-FAILED
     *                   path (a); {@code k > 0} drives the retry-exhaustion FAILED path (b).
     */
    public FailingWorkflow(CountingEffects effects, int maxRetries) {
        this.effects = effects;
        this.maxRetries = maxRetries;
    }

    /**
     * Workflow body: record one successful step, run an always-throwing step (which records a terminal {@code FAILED}
     * step status), then propagate that failure to a terminal FAILED workflow status via {@code ctx.fail(cause)}. The
     * {@code ctx.fail} primitive is terminal (§3.5) and ends the workflow immediately, so {@link #STEP_AFTER_FAILURE}
     * never begins. Re-run from the start on every (re)execution; on replay the recorded {@code reserveInventory} and
     * {@code failingStep} return their cached results and the already-FAILED instance re-reaches {@code ctx.fail} as a
     * no-op (nothing new is appended after the terminal status).
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

        // Always-throwing step, run via execute(...).await() so the step records a terminal FAILED status without the
        // raw StepFailedException unwinding the body (which the engine would only log, leaving the workflow
        // non-terminal). With maxRetries==0 the step fails on its first (only) attempt (path a); with maxRetries==k>0 it
        // records STARTED + RETRYING×k + FAILED and exhausts the policy (path b).
        WorkflowStepResult failing = ctx.execute(
                STEP_FAILING,
                Map.of(),
                (pc, payload) -> {
                    int attempt = effects.record(workflowId, STEP_FAILING);
                    throw new IllegalStateException("failingStep always fails (attempt " + attempt + ")");
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(maxRetries))
        );
        failing.await();

        // Propagate the step failure to a terminal FAILED workflow status — the supported terminal primitive (§3.5).
        // The step always fails, so this branch is always taken; ctx.fail ends the workflow immediately, so the
        // afterFailure step below never begins. (Guarded by failure() so the intent — fail iff the step failed — is
        // explicit; for this always-failing step the guard is always true, making the FAILED terminus deterministic.)
        if (failing.failure()) {
            ctx.fail(failing.error()
                            .map(e -> (Throwable) e)
                            .orElseGet(() -> new IllegalStateException("failingStep failed")));
        }

        // Unreachable on a correct engine: ctx.fail above terminated the workflow FAILED, so this step must never begin.
        // Its appearance in the committed log would be a FailurePropagation (and INV-7) break.
        ctx.awaitExecute(
                STEP_AFTER_FAILURE,
                Map.of(),
                (pc, payload) -> Map.of("after", effects.record(workflowId, STEP_AFTER_FAILURE))
        );
    }
}
