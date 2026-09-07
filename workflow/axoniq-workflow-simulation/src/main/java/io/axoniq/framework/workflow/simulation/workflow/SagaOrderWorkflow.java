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
import io.axoniq.framework.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FulfillmentConfirmedEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * A realistic production <strong>order-fulfillment saga with compensation</strong> — the Phase-1 production-realism
 * workload. Forward path: reserve stock, charge payment (with retries), await an external fulfillment confirmation
 * under a timeout, notify the customer. The two catch branches run <strong>compensation steps</strong> before
 * terminating — the pattern every saga-style production workflow uses and the existing workload never exercised
 * (steps executing <em>after</em> a FAILED/TIMED_OUT step, inside a catch block, followed by a terminal primitive):
 * <ul>
 *   <li>{@link StepTimedOutException} (fulfillment never confirmed): stock was reserved AND payment charged, so
 *       compensate <em>both</em> ({@code releaseStock}, {@code refundPayment}) and {@code ctx.cancel()} — the order is
 *       cancelled, not failed;</li>
 *   <li>{@link StepFailedException} (charge declined or indeterminate): nothing was charged, so release the stock only
 *       and {@code ctx.fail(...)}.</li>
 * </ul>
 * Branch selection is replay-deterministic by construction: both exception types are derived from the cached step
 * <em>state</em> ({@code result.timeout()} → {@code StepTimedOutException}, a subtype of {@code StepFailedException}),
 * so a replayed body re-takes the same branch the live run took.
 * <p>
 * The {@code mode} payload field ({@link #MODE_HAPPY} / {@link #MODE_CHARGE_DECLINED}) selects the charge action's
 * behaviour deterministically from the workflow payload. The compensation steps' retry policy is injected: the
 * <strong>no-retry</strong> variant models the fragile-but-common authoring (a compensation step interrupted by a
 * crash resolves indeterminate and its {@code StepFailedException} escapes the catch block <em>uncaught</em>), the
 * <strong>retrying</strong> variant models the recommended authoring (an interrupted compensation attempt resolves to
 * {@code RETRYING} + a fresh attempt and the saga still terminates cleanly). The Phase-1 scenarios contrast the two
 * under the same crash window.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class SagaOrderWorkflow {

    /**
     * Logical workflow name of the <strong>no-compensation-retry</strong> variant.
     */
    public static final String WORKFLOW_NAME_NO_COMP_RETRY = "SagaOrderWorkflow";

    /**
     * Logical workflow name of the <strong>compensation-retry</strong> variant.
     */
    public static final String WORKFLOW_NAME_COMP_RETRY = "SagaRetryCompOrderWorkflow";

    /**
     * Logical workflow name of the <strong>default-per-attempt-timeout</strong> variant (the doomed-attempt probe).
     */
    public static final String WORKFLOW_NAME_DEFAULT_TIMEOUT = "SagaDefaultTimeoutOrderWorkflow";

    /**
     * Forward step: reserves stock — first counting side effect.
     */
    public static final String STEP_RESERVE_STOCK = "reserveStock";

    /**
     * Forward step: charges payment under {@code maxRetries(}{@link #CHARGE_MAX_RETRIES}{@code )} — counting side
     * effect; in {@link #MODE_CHARGE_DECLINED} its action throws on every attempt and the step exhausts to FAILED.
     * Named {@code chargeCard} (NOT {@code chargePayment}) so the {@code WriteThenVanishFault} — which arms the
     * {@code OrderWorkflow}'s {@code chargePayment} COMPLETED by step name — cannot hit the saga's charge when both
     * ride the shared fuzz workload.
     */
    public static final String STEP_CHARGE_PAYMENT = "chargeCard";

    /**
     * Forward step: waits for the external {@link FulfillmentConfirmedEvent} correlated by {@code orderId}, under
     * {@link #FULFILLMENT_TIMEOUT}.
     */
    public static final String STEP_AWAIT_FULFILLMENT = "awaitFulfillment";

    /**
     * Forward step: notifies the customer — counting side effect; only reached on the happy path.
     */
    public static final String STEP_NOTIFY_CUSTOMER = "notifyCustomer";

    /**
     * Compensation step: releases the reserved stock — counting side effect; first step of BOTH catch branches.
     */
    public static final String STEP_RELEASE_STOCK = "releaseStock";

    /**
     * Compensation step: refunds the charged payment — counting side effect; only on the timeout branch (the failure
     * branch never charged).
     */
    public static final String STEP_REFUND_PAYMENT = "refundPayment";

    /**
     * The wait timeout on {@link #STEP_AWAIT_FULFILLMENT}: generous enough that ordinary settle nudges never trip it,
     * small enough that a deliberate {@code advanceTime} fires it deterministically.
     */
    public static final Duration FULFILLMENT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * {@code maxRetries} on {@link #STEP_CHARGE_PAYMENT} — so the declined-charge path exercises retry exhaustion
     * before failing, like a real payment integration.
     */
    public static final int CHARGE_MAX_RETRIES = 1;

    /**
     * {@code maxRetries} the <strong>compensation-retry</strong> variant puts on its compensation steps.
     */
    public static final int COMPENSATION_MAX_RETRIES = 2;

    /**
     * Mode: every step succeeds; the saga completes once the fulfillment confirmation arrives.
     */
    public static final String MODE_HAPPY = "happy";

    /**
     * Mode: the charge action throws on every attempt — the saga takes the failure-compensation branch.
     */
    public static final String MODE_CHARGE_DECLINED = "chargeDeclined";

    /**
     * The engine's documented default per-attempt step timeout (5 seconds) — the doomed-attempt variant pins this
     * explicitly so its behaviour does not silently drift if the engine default changes.
     */
    public static final Duration DEFAULT_STEP_TIMEOUT = Duration.ofSeconds(5);

    /**
     * The generous per-attempt timeout the standard variants put on every {@code execute} step. Needed because firing
     * the {@link #FULFILLMENT_TIMEOUT} wait deterministically requires advancing the virtual clock INTO the wall-clock
     * era of the statically-stamped STARTED timestamps (the D5/ARCHITECTURE §12 un-injectable
     * {@code GenericEventMessage} clock residual) — after that advance the virtual clock sits AHEAD of the wall stamps,
     * and any subsequent step under the 5s default would be judged past-deadline at entry (the doomed-attempt
     * mechanism the {@link #WORKFLOW_NAME_DEFAULT_TIMEOUT} variant pins on purpose). 365 days keeps the standard
     * variants probing what they are designed to probe (compensation under crash windows, not the timeout edge).
     */
    public static final Duration GENEROUS_STEP_TIMEOUT = Duration.ofDays(365);

    private final CountingEffects effects;
    private final RetryPolicy compensationRetryPolicy;
    private final Duration stepTimeout;

    /**
     * Creates the saga bound to the given effect registry, compensation retry policy and per-attempt step timeout.
     *
     * @param effects                 registry that survives crashes; every action bumps a counter here.
     * @param compensationRetryPolicy retry policy applied to BOTH compensation steps ({@code RetryPolicy.NONE} for the
     *                                fragile variant, a bounded policy for the recommended variant).
     * @param stepTimeout             per-attempt timeout applied to every {@code execute} step
     *                                ({@link #GENEROUS_STEP_TIMEOUT} for the standard variants,
     *                                {@link #DEFAULT_STEP_TIMEOUT} for the doomed-attempt probe).
     */
    public SagaOrderWorkflow(CountingEffects effects, RetryPolicy compensationRetryPolicy,
                             Duration stepTimeout) {
        this.effects = effects;
        this.compensationRetryPolicy = compensationRetryPolicy;
        this.stepTimeout = stepTimeout;
    }

    /**
     * The fragile-but-common variant: compensation steps carry NO retry policy, so a compensation attempt interrupted
     * by a crash resolves indeterminate and its failure escapes the catch block uncaught.
     *
     * @param effects registry that survives crashes.
     * @return the no-compensation-retry saga.
     */
        public static SagaOrderWorkflow withoutCompensationRetry(CountingEffects effects) {
        return new SagaOrderWorkflow(effects, RetryPolicy.NONE, GENEROUS_STEP_TIMEOUT);
    }

    /**
     * The recommended variant: compensation steps retry under
     * {@code maxRetries(}{@link #COMPENSATION_MAX_RETRIES}{@code )}, so a crash-interrupted compensation attempt
     * resolves to {@code RETRYING} + a fresh attempt and the saga still terminates cleanly.
     *
     * @param effects registry that survives crashes.
     * @return the compensation-retry saga.
     */
        public static SagaOrderWorkflow withCompensationRetry(CountingEffects effects) {
        return new SagaOrderWorkflow(effects,
                                     RetryPolicy.maxRetries(COMPENSATION_MAX_RETRIES)
                                                .withBackoff(BackoffStrategy.fixed(Duration.ofMillis(100))),
                                     GENEROUS_STEP_TIMEOUT);
    }

    /**
     * The doomed-attempt probe: every step keeps the engine's DEFAULT per-attempt timeout, so a step entered after
     * the clock has moved past {@code STARTED + timeout} (a forward clock jump in production; the era-crossing
     * virtual advance in the harness) exposes the engine's dispatch-before-deadline-check behaviour.
     *
     * @param effects registry that survives crashes.
     * @return the default-timeout saga.
     */
        public static SagaOrderWorkflow withDefaultTimeouts(CountingEffects effects) {
        return new SagaOrderWorkflow(effects, RetryPolicy.NONE, DEFAULT_STEP_TIMEOUT);
    }

    /**
     * Workflow body. Re-run from the start on every (re)execution; primitives return cached results for recorded
     * steps. The catch branches are replay-deterministic because the exception type is derived from cached step state.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");
        Object mode = ctx.workflowPayload().get("mode");

        try {
            ctx.awaitExecute(
                    STEP_RESERVE_STOCK,
                    Map.of(),
                    (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_STOCK)),
                    step -> step.timeout(stepTimeout)
            );

            ctx.awaitExecute(
                    STEP_CHARGE_PAYMENT,
                    Map.of(),
                    (pc, payload) -> {
                        effects.record(workflowId, STEP_CHARGE_PAYMENT);
                        if (MODE_CHARGE_DECLINED.equals(mode)) {
                            throw new IllegalStateException("card declined for order " + orderId);
                        }
                        return Map.of("charged", true);
                    },
                    step -> step.timeout(stepTimeout)
                                .retryPolicy(RetryPolicy.maxRetries(CHARGE_MAX_RETRIES)
                                                        .withBackoff(BackoffStrategy.fixed(Duration.ofMillis(100))))
            );

            ctx.awaitEvent(
                    STEP_AWAIT_FULFILLMENT,
                    FulfillmentConfirmedEvent.class,
                    associate(payloadProperty("orderId"), equalsTo(orderId)),
                    step -> step.timeout(FULFILLMENT_TIMEOUT)
            );

            ctx.awaitExecute(
                    STEP_NOTIFY_CUSTOMER,
                    Map.of(),
                    (pc, payload) -> Map.of("notified", effects.record(workflowId, STEP_NOTIFY_CUSTOMER)),
                    step -> step.timeout(stepTimeout)
            );
        } catch (StepTimedOutException e) {
            // Fulfillment never confirmed: stock was reserved AND payment charged — compensate both, cancel the order.
            compensate(ctx, workflowId, true);
            ctx.cancel();
        } catch (StepInterruptedException e) {
            throw e; // an engine interrupt is not a step failure: the step resumes on the next start
        } catch (StepFailedException e) {
            // Charge declined (or resolved indeterminate after a crash): nothing charged — release the stock, fail.
            compensate(ctx, workflowId, false);
            ctx.fail(e);
        }
    }

    /**
     * The compensation chain both catch branches share. Deliberately plain {@code awaitExecute} steps — exactly what a
     * production author writes — so a compensation step interrupted by a crash exercises the engine's indeterminate
     * resolution INSIDE a catch block.
     *
     * @param ctx        the workflow context.
     * @param workflowId the instance id, for the effect registry.
     * @param refund     whether the payment was charged and must be refunded (timeout branch) or never charged
     *                   (failure branch).
     */
    private void compensate(SimpleWorkflowContext ctx, String workflowId, boolean refund) {
        ctx.awaitExecute(
                STEP_RELEASE_STOCK,
                Map.of(),
                (pc, payload) -> Map.of("released", effects.record(workflowId, STEP_RELEASE_STOCK)),
                step -> step.timeout(stepTimeout).retryPolicy(compensationRetryPolicy)
        );
        if (refund) {
            ctx.awaitExecute(
                    STEP_REFUND_PAYMENT,
                    Map.of(),
                    (pc, payload) -> Map.of("refunded", effects.record(workflowId, STEP_REFUND_PAYMENT)),
                    step -> step.timeout(stepTimeout).retryPolicy(compensationRetryPolicy)
            );
        }
    }
}
