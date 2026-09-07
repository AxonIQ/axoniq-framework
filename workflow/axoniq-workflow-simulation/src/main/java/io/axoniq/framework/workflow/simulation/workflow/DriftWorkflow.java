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
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DriftSignalEvent;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * Two structurally-divergent bodies of one logical workflow, used to <strong>induce replay drift</strong> in the DST
 * harness and exercise INVARIANTS.md INV-18 ({@code DriftGuardPausesCleanly}).
 * <p>
 * Replay drift is what {@code WorkflowExecution.guardAgainstReplayDrift(stepName)} catches: new code running past a step
 * the recorded state already has terminal, <em>without</em> a {@code ctx.migrateVersion}. The guard compares the
 * runtime "book" (steps the current invocation has referenced — {@code referencedStepNames}) against the event-sourced
 * "book" (terminal steps in the recorded {@code state()}); if a terminal recorded step has not been referenced when a
 * <em>new</em> publishing primitive is about to append its first event, it throws {@link
 * io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowReplayDriftException} (see {@code WorkflowExecution.java}
 * {@code unreferencedTerminalSteps()}/{@code guardAgainstReplayDrift}, {@code :254-287}; the guard is called before the
 * first publish in {@code ExecuteDelegate.java:118}).
 * <p>
 * The two bodies are deliberately STRUCTURALLY DIVERGENT in exactly the way the guard catches:
 * <ul>
 *   <li>{@link #executeV1}: {@code reserveInventory} (execute) &rarr; {@code chargePayment} (execute, a counting side
 *       effect that becomes a <strong>recorded-terminal</strong> step) &rarr; {@code awaitSignal} (a never-arriving
 *       {@code waitForEvent}, so the instance stays LIVE / non-terminal with {@code chargePayment} durably COMPLETED in
 *       history). This is the "old code" that records the instance to the state the drift fires against.</li>
 *   <li>{@link #executeV2}: {@code reserveInventory} (execute, replays as a cached result) &rarr; {@code repackage} (a
 *       <strong>NEW</strong> execute step the v1 history has no record of). It deliberately <strong>skips</strong>
 *       {@code chargePayment}. This is the "new code", deployed WITHOUT a {@code ctx.migrateVersion}.</li>
 * </ul>
 * When the recovered engine replays the v1-recorded instance under {@link #executeV2}: {@code reserveInventory} is
 * referenced (cached), then the body reaches {@code repackage}, a step absent from state, so {@code ExecuteDelegate}
 * calls {@code guardAgainstReplayDrift("repackage")}. At that point the recorded-terminal {@code chargePayment} has
 * <em>not</em> been referenced by this v2 invocation (v2 never calls it), so it is an unreferenced terminal step &rarr;
 * the guard throws {@code WorkflowReplayDriftException}. {@code SimpleWorkflowExecution.handleWorkflowException} catches
 * it and PAUSES the instance NON-TERMINALLY: it logs a warning and intentionally publishes <strong>no</strong> terminal
 * workflow event and no {@code repackage} event ({@code :289-297}). That paused-clean state is exactly what INV-18
 * asserts.
 * <p>
 * Both bodies share {@code reserveInventory}/{@code chargePayment} step-name strings with {@link OrderWorkflow} so the
 * harness can reuse the {@link CountingEffects} key shape; the workflow name differs so the definitions never collide.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class DriftWorkflow {

    /**
     * Logical workflow name, stable and distinct from {@link OrderWorkflow#WORKFLOW_NAME}. Both the v1 and v2 bodies
     * register under this same name (the divergence is in the body, not the name — a step rename would be a forbidden
     * migration, not a drift).
     */
    public static final String WORKFLOW_NAME = "DriftWorkflow";

    /**
     * The single semver version both bodies register under. The v1&rarr;v2 change is a structural body divergence
     * deployed <strong>WITHOUT</strong> bumping the version or calling {@code ctx.migrateVersion} — which is exactly the
     * un-migrated drift the guard catches. Both register at the same version so the recovered engine re-routes the same
     * recorded instance to the divergent body (rather than spawning a new sibling version).
     */
    public static final String DRIFT_VERSION = "1.0.0";

    /**
     * First step, present and referenced by BOTH bodies (the v2 replay reaches it as a cached result), so it is never
     * the drift orphan.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    /**
     * The v1-only recorded-terminal step that v2 skips — the unreferenced terminal step the drift guard fires on.
     */
    public static final String STEP_CHARGE_PAYMENT = "chargePayment";

    /**
     * The v1 never-arriving wait that keeps the instance LIVE / non-terminal after {@code chargePayment} is recorded
     * terminal, so a recovered engine re-runs the body (rather than short-circuiting an already-terminal instance).
     */
    public static final String STEP_AWAIT_SIGNAL = "awaitSignal";

    /**
     * The NEW v2-only step that is absent from the v1-recorded history; reaching its publish-side guard with the
     * recorded-terminal {@code chargePayment} unreferenced is what trips {@code guardAgainstReplayDrift}.
     */
    public static final String STEP_REPACKAGE = "repackage";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; each {@code execute} body bumps a counter here.
     */
    public DriftWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The v1 ("old code") body: record {@code reserveInventory} then {@code chargePayment} (a counting side effect that
     * becomes a recorded-terminal step), then suspend on a never-arriving {@code waitForEvent} so the instance stays
     * LIVE / non-terminal with {@code chargePayment} durably COMPLETED in history.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV1(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        Object orderId = ctx.workflowPayload().get("orderId");

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

        // Never-arriving wait: keeps the instance LIVE / non-terminal (chargePayment is durably COMPLETED), so a
        // recovered engine running a divergent body re-executes from the start rather than short-circuiting a terminal
        // instance. The signal for this orderId is deliberately never delivered.
        ctx.awaitEvent(
                STEP_AWAIT_SIGNAL,
                DriftSignalEvent.class,
                associate(payloadProperty("orderId"), equalsTo(orderId)),
                step -> step.timeout(Duration.ofDays(365))
        );
    }

    /**
     * The v2 ("new code", deployed WITHOUT {@code ctx.migrateVersion}) body: replay {@code reserveInventory} (cached),
     * then reach the NEW {@code repackage} step, deliberately SKIPPING the recorded-terminal {@code chargePayment}. When
     * this runs against a v1-recorded instance, {@code chargePayment} is an unreferenced terminal step at the
     * {@code repackage} publish-side guard &rarr; {@code guardAgainstReplayDrift} throws and the engine pauses the
     * instance non-terminally and cleanly (INV-18).
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void executeV2(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        ctx.awaitExecute(
                STEP_RESERVE_INVENTORY,
                Map.of(),
                (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE_INVENTORY))
        );

        // chargePayment is INTENTIONALLY skipped here — the structural divergence the drift guard catches. Reaching the
        // new step's publish-side guard with chargePayment recorded-terminal-but-unreferenced trips the guard.
        ctx.awaitExecute(
                STEP_REPACKAGE,
                Map.of(),
                (pc, payload) -> Map.of("repackaged", effects.record(workflowId, STEP_REPACKAGE))
        );
    }
}
