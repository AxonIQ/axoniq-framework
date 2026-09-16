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

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * A workflow that parks twice, so its live state can move while the instance stays live.
 * <p>
 * The body waits on a first approval; when that wait is cancelled from outside it records a compensation effect and
 * parks again on a resume signal; only that signal completes it. Between the cancel and the signal the instance is
 * live with a state the history projection can be held behind — the one shape in which the manager's live-over-history
 * rule is observable, which is what {@code ManagerLiveWinsScenario} needs. Scenario-only: without the external cancel
 * and the signal the instance never terminates, so it is never folded into the fuzz defaults.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ManagerLiveWinsWorkflow {

    public static final String WORKFLOW_NAME = "ManagerLiveWinsWorkflow";
    public static final String STEP_AWAIT_APPROVAL = "awaitApproval";
    public static final String STEP_COMPENSATE = "compensate";
    public static final String STEP_AWAIT_RESUME = "awaitResume";
    /** Suffix appended to the order id to form the resume signal's correlation key. */
    public static final String RESUME_KEY_SUFFIX = ":resume";

    private final CountingEffects effects;

    /**
     * Creates the workflow body.
     *
     * @param effects counting side-effect registry the compensation records into.
     */
    public ManagerLiveWinsWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The workflow body.
     *
     * @param ctx the workflow context.
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
            ctx.awaitEvent(
                    STEP_AWAIT_RESUME,
                    CorrelatedSignalEvent.class,
                    associate(payloadProperty("key"), equalsTo(orderId + RESUME_KEY_SUFFIX)),
                    step -> step.timeout(Duration.ofDays(365)));
        }
    }
}
