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
import io.axoniq.framework.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Test workflow with counting side effects, designed so a deterministic simulator can observe effect duplication
 * (INVARIANTS.md INV-6 / F-0) and exercise every recovery surface.
 * <p>
 * Shape (deliberately covering the cases Phase 4 must stress):
 * <ol>
 *   <li>{@code reserveInventory} ({@code execute}) — a counting side effect;</li>
 *   <li>{@code chargePayment} ({@code execute}) — a second counting side effect, giving the required
 *       {@code execute}&rarr;{@code execute} sequence. This is the step the write-then-vanish fault targets: a crash
 *       after the charge runs but before its {@code COMPLETED} commits re-charges on replay;</li>
 *   <li>{@code awaitConfirmation} ({@code waitForEvent} with an {@code orderId} association) — suspends until the
 *       external {@link PaymentConfirmedEvent} arrives;</li>
 *   <li>{@code settleDelay} ({@code sleep}) — a durable delay fired by the virtual-time scheduler;</li>
 *   <li>{@code shipOrder} ({@code execute} with {@code RetryPolicy.maxRetries(2)} + backoff) — a counting side effect
 *       that <strong>genuinely retries</strong>: its action throws on the first two attempts and succeeds on the third,
 *       so the engine records {@code STARTED} then {@code RETRYING ×2} then {@code COMPLETED}. This is what exercises
 *       INVARIANTS.md INV-8 ({@code RetryBound}) — the number of attempt records ({@code STARTED}/{@code RETRYING}) for
 *       a {@code maxRetries(n)} step must stay {@code ≤ n+1} even when a crash/restart interrupts a retry. The
 *       failure decision is driven by the {@link CountingEffects} attempt counter (which survives a crash exactly like
 *       a real external effect), so under the seeded faults the retry path is reproducibly crashed.</li>
 * </ol>
 * The action lambdas are the only place side effects live, and every body increments the matching
 * {@link CountingEffects} counter so the harness can assert effect-at-most-once (or document the F-0 gap).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class OrderWorkflow {

    /**
     * Logical workflow name, stable across versions and reused by the harness when registering the definition.
     */
    public static final String WORKFLOW_NAME = "OrderWorkflow";

    /**
     * Step that reserves inventory — first counting side effect.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * Step that charges payment — second counting side effect and the write-then-vanish (F-0) target.
     */
    public static final String STEP_CHARGE_PAYMENT = "chargePayment";

    /**
     * Step that waits for the external payment confirmation.
     */
    public static final String STEP_AWAIT_CONFIRMATION = "awaitConfirmation";

    /**
     * Durable delay step.
     */
    public static final String STEP_SETTLE_DELAY = "settleDelay";

    /**
     * Step that ships the order with a retry policy — counting side effect on the retry path.
     */
    public static final String STEP_SHIP_ORDER = "shipOrder";

    /**
     * The {@code maxRetries} configured on {@link #STEP_SHIP_ORDER}. Exposed so the harness can assert INV-8
     * ({@code RetryBound}) against the policy bound without hard-coding the number: the engine must never record more
     * than {@code SHIP_ORDER_MAX_RETRIES + 1} attempt events for this step (one {@code STARTED} + at most this many
     * {@code RETRYING}).
     */
    public static final int SHIP_ORDER_MAX_RETRIES = 2;

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every {@code execute} body bumps a counter here.
     */
    public OrderWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body. Re-run from the start on every (re)execution; primitives return cached results for steps already
     * recorded in state, and only absent steps run live.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        try {
            ctx.awaitExecute(
                    STEP_RESERVE_INVENTORY,
                    Map.of(),
                    (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY))
            );

            ctx.awaitExecute(
                    STEP_CHARGE_PAYMENT,
                    Map.of(),
                    (pc, payload) -> Map.of("charged", effects.record(workflowId, STEP_CHARGE_PAYMENT))
            );

            ctx.awaitEvent(
                    STEP_AWAIT_CONFIRMATION,
                    PaymentConfirmedEvent.class,
                    associate(payloadProperty("orderId"), equalsTo(orderId)),
                    // An effectively-infinite wait timeout (365 days): awaitConfirmation is a pure wait-for-event, not a
                    // race against the clock. This keeps the step robust to the CLOCK_JUMP fault and to settle's
                    // timer-draining (both advance virtual time, but never by anywhere near 365 days), so the wait is
                    // resolved by the delivered confirmation rather than spuriously timing out on resume after a clock
                    // jump. (Wait-timeout behaviour itself is exercised by the dsl/examples suite, not this harness.)
                    step -> step.timeout(Duration.ofDays(365))
            );

            ctx.sleep(STEP_SETTLE_DELAY, Duration.ofSeconds(1));

            ctx.awaitExecute(
                    STEP_SHIP_ORDER,
                    Map.of(),
                    (pc, payload) -> {
                        // Genuinely retry: fail the first SHIP_ORDER_MAX_RETRIES attempts, succeed on the next. The
                        // attempt count is the persistent CountingEffects counter (it survives a crash, like a real
                        // external effect), so the engine emits STARTED then RETRYING up to SHIP_ORDER_MAX_RETRIES times
                        // then COMPLETED — the attempt-record sequence INV-8 (RetryBound) bounds at
                        // SHIP_ORDER_MAX_RETRIES + 1.
                        int attempt = effects.record(workflowId, STEP_SHIP_ORDER);
                        if (attempt <= SHIP_ORDER_MAX_RETRIES) {
                            throw new IllegalStateException(
                                    "shipOrder transient failure on attempt " + attempt + " (forces a retry)");
                        }
                        return Map.of("shipped", attempt);
                    },
                    step -> step.retryPolicy(RetryPolicy.maxRetries(SHIP_ORDER_MAX_RETRIES)
                                                        .withBackoff(BackoffStrategy.fixed(Duration.ofMillis(200))))
            );
        } catch (StepFailedException e) {
            // AT-MOST-ONCE (F-0): a step whose action was in-flight at a crash is NOT re-run on recovery; it surfaces
            // here as a StepFailedException (cause StepIndeterminateException). The engine does not auto-fail the
            // workflow on an unhandled exception (by design — only ctx.fail terminates), so propagate it explicitly to a
            // terminal FAILED status (the FailingWorkflow pattern). The happy path never throws, so this is inert there.
            ctx.fail(e);
        }
    }
}
