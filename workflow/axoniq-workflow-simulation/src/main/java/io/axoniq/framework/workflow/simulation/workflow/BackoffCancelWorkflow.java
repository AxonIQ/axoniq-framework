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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * A workflow that launches a retrying {@code execute} step whose first attempt fails (putting the step into a durable
 * {@code RETRYING} state with a long fixed backoff), parks on a correlated wait, and then — depending on the
 * {@code mode} payload field — either cancels the whole workflow from its own body ({@link #MODE_IN_BODY_CANCEL}) or
 * completes normally ({@link #MODE_EXTERNAL}, where the scenario cancels the retrying step externally instead).
 * <p>
 * Purpose: exercise <strong>cancellation during a retry backoff window</strong>. While a step waits out its backoff,
 * the only registered running future is the backoff-launch future
 * ({@code RetryableExecuteDelegate.scheduleRetryAttempt}), which has no cancellation-to-publish wiring and whose
 * scheduled launch task is never unscheduled. So:
 * <ul>
 *   <li>{@link #MODE_IN_BODY_CANCEL}: {@code ctx.cancel(...)} → {@code TerminateDelegate.terminate} →
 *       {@code cancelAllRunningSteps} completes the backoff future exceptionally and then blocks in
 *       {@code awaitStateChange(allTerminal)} — but nothing ever publishes a terminal record for the RETRYING step, so
 *       the cancel is held hostage until the backoff elapses, the "cancelled" step's next attempt RUNS its side effect
 *       anyway, and only after the step reaches its own terminal does the workflow record CANCELLED;</li>
 *   <li>{@link #MODE_EXTERNAL}: an external {@code cancelRunningStep(...)} completes the backoff future exceptionally
 *       and is silently lost — no CANCELLED record is published, the step stays durably RETRYING, and the scheduled
 *       retry still runs its side effect when the backoff elapses.</li>
 * </ul>
 * The flaky step's action fails on attempt 1 and succeeds on attempt 2 (driven by the crash-surviving
 * {@link CountingEffects} attempt counter — deterministic, axon-flow-workflow skill §3.3), and carries an
 * effectively-infinite per-attempt timeout so the virtual-time advance that fires the backoff cannot trip the
 * per-attempt deadline (keeping the F-14 doomed-attempt path out of this scenario).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class BackoffCancelWorkflow {

    /**
     * Logical workflow name (single definition), stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "BackoffCancelWorkflow";

    /**
     * The retrying step: fails on attempt 1, succeeds on attempt 2, fixed {@link #BACKOFF_DELAY} backoff between them.
     */
    public static final String STEP_FLAKY = "flakyCharge";

    /**
     * The correlated wait that parks the body while the flaky step sits in its backoff window.
     */
    public static final String STEP_AWAIT_DECISION = "awaitDecision";

    /**
     * Mode: after the wait resolves, the body cancels the whole workflow via {@code ctx.cancel(...)} — while the flaky
     * step is still in its backoff window.
     */
    public static final String MODE_IN_BODY_CANCEL = "cancel";

    /**
     * Mode: the body completes normally after the wait; the scenario cancels the retrying step externally instead.
     */
    public static final String MODE_EXTERNAL = "external";

    /**
     * The fixed backoff before the flaky step's retry attempt — long enough that it only elapses when the scenario
     * advances virtual time explicitly.
     */
    public static final Duration BACKOFF_DELAY = Duration.ofSeconds(60);

    /**
     * Max retries on the flaky step (attempt 2 succeeds, so the bound is never exhausted).
     */
    public static final int MAX_RETRIES = 2;

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the flaky action bumps a per-attempt counter here.
     */
    public BackoffCancelWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: launch the retrying flaky step (not awaited), park on the correlated wait, then cancel or complete
     * per the {@code mode} payload field.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        String orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
        String mode = String.valueOf(ctx.workflowPayload().get("mode"));

        // Deliberately NOT awaited: the step must sit in its backoff window while the body moves on to the wait.
        ctx.execute(
                STEP_FLAKY,
                Map.of(),
                (pc, payload) -> {
                    int attempt = effects.record(workflowId, STEP_FLAKY);
                    if (attempt <= 1) {
                        throw new IllegalStateException("flakyCharge fails on attempt " + attempt);
                    }
                    return Map.of("charged", attempt);
                },
                step -> step.timeout(Duration.ofDays(365))
                            .retryPolicy(RetryPolicy.maxRetries(MAX_RETRIES)
                                                    .withBackoff(BackoffStrategy.fixed(BACKOFF_DELAY))));

        ctx.awaitEvent(
                STEP_AWAIT_DECISION,
                CorrelatedSignalEvent.class,
                associate(payloadProperty("key"), equalsTo(orderId)),
                step -> step.timeout(Duration.ofDays(365)));

        if (MODE_IN_BODY_CANCEL.equals(mode)) {
            ctx.cancel("cancel requested during backoff");
        }
        // MODE_EXTERNAL: return normally — the externally-cancelled flaky step is deliberately left un-awaited.
    }
}
