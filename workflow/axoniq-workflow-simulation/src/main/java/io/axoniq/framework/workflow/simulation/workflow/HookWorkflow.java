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

import java.util.Map;

/**
 * Minimal workflow that reaches at least the STARTED and a terminal COMPLETED <strong>workflow status</strong>, used to
 * exercise INVARIANTS.md INV-17 ({@code StatusHookFiresOncePerStatus}) — that a registered status-change hook fires
 * exactly once per status transition for an instance and is NOT re-fired on crash/replay.
 * <p>
 * It runs one recorded {@code execute} step ({@code doWork}, a counting side effect, so there is a real step record
 * <em>between</em> STARTED and COMPLETED) and then returns, so the engine emits the workflow-status events STARTED then
 * COMPLETED. A counting {@link io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener
 * status-change listener} (see {@link io.axoniq.framework.workflow.simulation.harness.EngineInstance#hookWorkflow}) is registered
 * on STARTED and on COMPLETED via the {@code .customized(...)} path, recording each fire into a crash-surviving
 * {@link StatusHookFires} counter. INV-17 then checks that across the instance's whole lifetime — including a crash +
 * replay that re-runs the body and re-drives the engine's event evolution — each registered status fired exactly once.
 * <p>
 * This is the lifecycle-hook analogue of INV-6 ({@code EffectAtMostOnce}/F-0): a status hook is a user side effect, and
 * replaying the committed status events must not re-invoke it. The step name ({@link #STEP_DO_WORK}) and effect-counter
 * shape mirror the other simulation workflows; the workflow name differs so this definition never collides with another.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class HookWorkflow {

    /**
     * Logical workflow name, stable and distinct from the other simulation workflows.
     */
    public static final String WORKFLOW_NAME = "HookWorkflow";

    /**
     * The single recorded step that runs (a counting side effect) between STARTED and COMPLETED.
     */
    public static final String STEP_DO_WORK = "doWork";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the single {@code execute} body bumps a counter here.
     */
    public HookWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body: run one recorded step, then return (reaching COMPLETED). Re-run from the start on every
     * (re)execution; on replay the recorded {@code doWork} returns its cached result and the body completes again, which
     * a correct engine treats as already-COMPLETED (no new step/status event recorded). The question INV-17 asks is
     * whether the registered STARTED/COMPLETED hooks re-fire while the engine re-evolves the committed status events on
     * replay.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(
                STEP_DO_WORK,
                Map.of(),
                (pc, payload) -> Map.of("worked", effects.record(workflowId, STEP_DO_WORK))
        );
        // Body returns here -> the engine emits the terminal COMPLETED workflow status.
    }
}
