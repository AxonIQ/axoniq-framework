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
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * A workflow that suspends on a blocking {@code awaitEvent}, catches the {@link StepCancellationException} the blocking
 * convenience surfaces when that step is cancelled, and then runs a compensation {@code execute} step to normal
 * completion — the catch-and-compensate shape. Used to exercise cancellation of a running step from a thread other
 * than the workflow's own control thread: after an external {@code cancelRunningStep(...)}, the body must observe the
 * cancellation, run {@link #STEP_COMPENSATE}, and the workflow must reach a terminal {@code COMPLETED} status.
 * <p>
 * Shape:
 * <ol>
 *   <li>{@link #STEP_AWAIT_APPROVAL} ({@code awaitEvent} on {@link CorrelatedSignalEvent}, correlated via
 *       {@code associate(payloadProperty("key"), equalsTo(orderId))}, effectively-infinite 365-day timeout) — suspends
 *       the instance; the scenario never publishes the signal, so the only way past this step is cancellation;</li>
 *   <li>on {@link StepCancellationException}: {@link #STEP_COMPENSATE} ({@code execute}) — a counting side effect
 *       recorded into the crash-surviving {@link CountingEffects}, so the committed log plus the effect registry reveal
 *       whether compensation actually ran after the external cancellation.</li>
 * </ol>
 * Determinism (axon-flow-workflow skill §3.3): the body reads only its own payload — no wall-clock, randomness, or
 * external state — so the committed subsequence is a pure function of the history.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ExternalCancelCompensationWorkflow {

    /**
     * Logical workflow name (single definition), stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "ExternalCancelCompensationWorkflow";

    /**
     * The blocking wait step the scenario cancels externally; its signal is never published.
     */
    public static final String STEP_AWAIT_APPROVAL = "awaitApproval";

    /**
     * The compensation step the body runs after catching the wait step's cancellation.
     */
    public static final String STEP_COMPENSATE = "compensate";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the compensation body bumps a counter here.
     */
    public ExternalCancelCompensationWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: suspend on the blocking {@code awaitEvent}, catch its cancellation, compensate, complete.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        String orderId = String.valueOf(ctx.workflowPayload().get("orderId"));

        try {
            ctx.awaitEvent(
                    STEP_AWAIT_APPROVAL,
                    CorrelatedSignalEvent.class,
                    associate(payloadProperty("key"), equalsTo(orderId)),
                    step -> step.timeout(Duration.ofDays(365)));
        } catch (StepCancellationException cancelled) {
            ctx.awaitExecute(
                    STEP_COMPENSATE,
                    Map.of(),
                    (pc, payload) -> {
                        effects.record(workflowId, STEP_COMPENSATE);
                        return Map.of("compensated", true);
                    });
        }
    }
}
