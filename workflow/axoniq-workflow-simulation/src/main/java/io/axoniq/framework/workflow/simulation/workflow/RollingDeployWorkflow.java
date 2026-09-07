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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ApprovalGrantedEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * The Phase-3 production-realism <strong>rolling-deploy</strong> workload: an order workflow that parks on an external
 * approval mid-flight while the code deploys around it. Three bodies model the deploy variants production actually
 * ships:
 * <ul>
 *   <li>{@link #executeV1 v1} — reserve → await approval → fulfill;</li>
 *   <li>{@link #executeV2 v2 (correct)} — the ADR-005 authoring: the new {@code fraudCheck} step is gated behind
 *       {@code ctx.migrateVersion(}{@link #CHANGE_ID}{@code , }{@link #VERSION_V2}{@code )}, so an in-flight v1
 *       instance recovering under v2 forks deliberately and records the migration;</li>
 *   <li>{@link #executeV2Bad v2-bad} — the authoring mistake: the same structural change WITHOUT
 *       {@code migrateVersion}. Because the parked instance's only post-insertion record is its non-terminal wait
 *       (STARTED), the drift guard's unreferenced-TERMINAL-step predicate has nothing to trip on — the new step runs
 *       silently on recovery (the blind spot the Phase-3 scenario characterizes; the SAME divergence past a terminal
 *       step pauses, INV-18).</li>
 * </ul>
 * Execute steps carry generous per-attempt timeouts ({@link SagaOrderWorkflow#GENEROUS_STEP_TIMEOUT} rationale — the
 * F-14/D5 era residual).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class RollingDeployWorkflow {

    /**
     * Logical workflow name shared by every version variant (ADR-005 sibling registrations).
     */
    public static final String WORKFLOW_NAME = "RollingDeployWorkflow";

    /**
     * The originally-shipped version.
     */
    public static final String VERSION_V1 = "1.0.0";

    /**
     * The deployed bump (same major — eligible for the closest-higher-sibling routing pass).
     */
    public static final String VERSION_V2 = "1.0.1";

    /**
     * The {@code migrateVersion} change id the correct v2 body records.
     */
    public static final String CHANGE_ID = "add-fraud-check";

    /**
     * Step that reserves inventory — counting side effect, present in every variant.
     */
    public static final String STEP_RESERVE = "reserveInventory";

    /**
     * The v2-only step — counting side effect; gated behind {@code migrateVersion} in the correct body, ungated in
     * the bad body.
     */
    public static final String STEP_FRAUD_CHECK = "fraudCheck";

    /**
     * The parked wait: suspends on the external {@link ApprovalGrantedEvent} correlated by {@code orderId}.
     */
    public static final String STEP_AWAIT_APPROVAL = "awaitApproval";

    /**
     * Step that fulfills the order — counting side effect, present in every variant.
     */
    public static final String STEP_FULFILL = "fulfillOrder";

    /**
     * The approval window — day-scale (the realistic long park a deploy meets).
     */
    public static final Duration APPROVAL_TIMEOUT = Duration.ofDays(30);

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes (and registry swaps); every action bumps a counter here.
     */
    public RollingDeployWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The v1 body: reserve → await approval → fulfill.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV1(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");
        try {
            reserve(ctx, workflowId);
            awaitApproval(ctx, orderId);
            fulfill(ctx, workflowId);
        } catch (StepTimedOutException e) {
            ctx.cancel();
        } catch (StepFailedException e) {
            // Fuzz-workload hardening (OrderWorkflow pattern): drive crash-resolved step failures to terminal FAILED.
            ctx.fail(e);
        }
    }

    /**
     * The correct v2 body: the new {@code fraudCheck} step is gated behind {@code ctx.migrateVersion}, per ADR-005.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV2(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");
        try {
            reserve(ctx, workflowId);
            if (ctx.migrateVersion(CHANGE_ID, VERSION_V2)) {
                fraudCheck(ctx, workflowId);
            }
            awaitApproval(ctx, orderId);
            fulfill(ctx, workflowId);
        } catch (StepTimedOutException e) {
            ctx.cancel();
        } catch (StepFailedException e) {
            ctx.fail(e);
        }
    }

    /**
     * The BAD v2 body: the same structural change WITHOUT {@code migrateVersion} — the authoring mistake whose silent
     * acceptance on a parked instance (and whose poisoning of a later rollback) the Phase-3 scenario characterizes.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV2Bad(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");
        try {
            reserve(ctx, workflowId);
            fraudCheck(ctx, workflowId);
            awaitApproval(ctx, orderId);
            fulfill(ctx, workflowId);
        } catch (StepTimedOutException e) {
            ctx.cancel();
        } catch (StepFailedException e) {
            ctx.fail(e);
        }
    }

    private void reserve(SimpleWorkflowContext ctx, String workflowId) {
        ctx.awaitExecute(
                STEP_RESERVE,
                Map.of(),
                (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE)),
                step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
        );
    }

    private void fraudCheck(SimpleWorkflowContext ctx, String workflowId) {
        ctx.awaitExecute(
                STEP_FRAUD_CHECK,
                Map.of(),
                (pc, payload) -> Map.of("checked", effects.record(workflowId, STEP_FRAUD_CHECK)),
                step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
        );
    }

    private void awaitApproval(SimpleWorkflowContext ctx, Object orderId) {
        ctx.awaitEvent(
                STEP_AWAIT_APPROVAL,
                ApprovalGrantedEvent.class,
                associate(payloadProperty("orderId"), equalsTo(orderId)),
                step -> step.timeout(APPROVAL_TIMEOUT)
        );
    }

    private void fulfill(SimpleWorkflowContext ctx, String workflowId) {
        ctx.awaitExecute(
                STEP_FULFILL,
                Map.of(),
                (pc, payload) -> Map.of("fulfilled", effects.record(workflowId, STEP_FULFILL)),
                step -> step.timeout(SagaOrderWorkflow.GENEROUS_STEP_TIMEOUT)
        );
    }
}
