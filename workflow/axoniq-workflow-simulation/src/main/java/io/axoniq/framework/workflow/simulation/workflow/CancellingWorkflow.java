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
 * Minimal workflow that reaches a <strong>terminal workflow status</strong> (CANCELLED), used to exercise
 * INVARIANTS.md INV-7 ({@code TerminalIsFinal}) — that once an instance records a terminal workflow status, no further
 * step or status events are ever recorded for it, even across a crash/replay.
 * <p>
 * The two existing test workflows ({@link OrderWorkflow}) only ever reach COMPLETED. INV-7 must also be exercised on
 * the {@code ctx.cancel}/{@code ctx.fail} terminal paths (axon-flow-workflow skill §3.5: those primitives end the
 * workflow immediately and nothing after them may run or record), so this workflow drives exactly that:
 * <ol>
 *   <li>{@code reserveInventory} ({@code execute}) — one counting side effect, so there is a real step record
 *       <em>before</em> termination (otherwise "no events after terminal" would be vacuous);</li>
 *   <li>{@code ctx.cancel()} — terminal: publishes the CANCELLED workflow-status event. No primitive follows it, so a
 *       correct engine records nothing further for this instance — that is precisely what INV-7 asserts.</li>
 * </ol>
 * The step name ({@link #STEP_RESERVE_INVENTORY}) is deliberately the same string as {@code OrderWorkflow}'s first
 * step so the harness can reuse the {@link CountingEffects} key shape; the workflow name differs so the two
 * definitions never collide.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class CancellingWorkflow {

    /**
     * Logical workflow name, stable and distinct from {@link OrderWorkflow#WORKFLOW_NAME}.
     */
    public static final String WORKFLOW_NAME = "CancellingWorkflow";

    /**
     * The single step that runs (a counting side effect) before the workflow cancels itself.
     */
    public static final String STEP_RESERVE_INVENTORY = "reserveInventory";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the single {@code execute} body bumps a counter here.
     */
    public CancellingWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body: run one recorded step, then cancel. Re-run from the start on every (re)execution; on replay the
     * recorded {@code reserveInventory} returns its cached result and the terminal {@code cancel} is reached again,
     * which a correct engine treats as a no-op once the instance is already terminal (no new event recorded).
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

        // Terminal primitive (§3.5): publishes the CANCELLED workflow-status event and ends the workflow. Nothing may
        // run or record after this — the property INV-7 (TerminalIsFinal) checks.
        ctx.cancel();
    }
}
