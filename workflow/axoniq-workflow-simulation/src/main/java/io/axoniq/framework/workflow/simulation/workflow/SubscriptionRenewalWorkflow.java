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
import io.axoniq.framework.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RenewalDecidedEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * The Phase-2 production-realism <strong>long-parked</strong> workload: a subscription-renewal workflow that registers
 * a subscription and then parks for up to {@link #DECISION_TIMEOUT 30 days} on an external
 * {@link RenewalDecidedEvent renewal decision} — the day-scale wait every real deployment carries (renewals, payment
 * reminders, document-signing windows) while short-lived instances churn around it and the process restarts under it.
 * <ul>
 *   <li>decision arrives in time → {@code processRenewal} → COMPLETED;</li>
 *   <li>no decision within the window → {@code expireSubscription} → {@code ctx.cancel()} (the subscription lapses).</li>
 * </ul>
 * Execute steps carry a generous per-attempt timeout ({@link SagaOrderWorkflow#GENEROUS_STEP_TIMEOUT} rationale): the
 * scenarios deterministically fire the 30-day wait timeout via the era-anchored virtual advance, after which any
 * default-timeout step would be past-deadline at entry (the documented F-14 mechanism — probed by the P1 campaign,
 * deliberately NOT re-probed here).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class SubscriptionRenewalWorkflow {

    /**
     * Logical workflow name, stable and distinct from every other test workflow's name.
     */
    public static final String WORKFLOW_NAME = "SubscriptionRenewalWorkflow";

    /**
     * Step that registers the subscription — counting side effect.
     */
    public static final String STEP_REGISTER = "registerSubscription";

    /**
     * The long-parked wait: suspends on the external {@link RenewalDecidedEvent} correlated by {@code orderId}, under
     * the {@link #DECISION_TIMEOUT 30-day} window.
     */
    public static final String STEP_AWAIT_DECISION = "awaitRenewalDecision";

    /**
     * Step that processes the renewal — counting side effect; only on the decided path.
     */
    public static final String STEP_PROCESS_RENEWAL = "processRenewal";

    /**
     * Step that expires the subscription — counting side effect; only on the timeout path.
     */
    public static final String STEP_EXPIRE = "expireSubscription";

    /**
     * The renewal-decision window: day-scale on purpose (the realistic long park), fired deterministically by the
     * scenarios via the era-anchored virtual advance.
     */
    public static final Duration DECISION_TIMEOUT = Duration.ofDays(30);

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; every action bumps a counter here.
     */
    public SubscriptionRenewalWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body. Re-run from the start on every (re)execution; on replay the recorded steps return cached results
     * and a still-STARTED wait re-registers its condition with the remaining window.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

        try {
            ctx.awaitExecute(
                    STEP_REGISTER,
                    Map.of(),
                    (pc, payload) -> Map.of("registered", effects.record(workflowId, STEP_REGISTER)),
                    step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
            );

            ctx.awaitEvent(
                    STEP_AWAIT_DECISION,
                    RenewalDecidedEvent.class,
                    associate(payloadProperty("orderId"), equalsTo(orderId)),
                    step -> step.timeout(DECISION_TIMEOUT)
            );

            ctx.awaitExecute(
                    STEP_PROCESS_RENEWAL,
                    Map.of(),
                    (pc, payload) -> Map.of("renewed", effects.record(workflowId, STEP_PROCESS_RENEWAL)),
                    step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
            );
        } catch (StepTimedOutException e) {
            ctx.awaitExecute(
                    STEP_EXPIRE,
                    Map.of(),
                    (pc, payload) -> Map.of("expired", effects.record(workflowId, STEP_EXPIRE)),
                    step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
            );
            ctx.cancel();
        } catch (StepFailedException e) {
            // Fuzz-workload hardening (the OrderWorkflow pattern): a crash-interrupted execute step resolves through
            // the engine's at-most-once flow to FAILED (StepIndeterminateException); propagate explicitly to a
            // terminal FAILED so the instance never hits the F-15 default-sink wedge under the shared fault campaign.
            ctx.fail(e);
        }
    }
}
