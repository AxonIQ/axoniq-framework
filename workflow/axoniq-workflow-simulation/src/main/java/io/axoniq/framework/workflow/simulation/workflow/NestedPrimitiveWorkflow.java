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
 * A DELIBERATELY-MISUSED workflow that violates the {@code axon-flow-workflow} skill §3.1 rule ("No nested primitives"):
 * its outer {@code execute} action lambda calls ANOTHER primitive ({@code ctx.awaitExecute(...)}) from inside the
 * action. This is the canonical forbidden anti-pattern the skill warns "deadlocks":
 *
 * <pre>{@code
 * // ✗ FORBIDDEN — nested primitive (this workflow does exactly this)
 * ctx.awaitExecute("outer", (pc, p) -> {
 *     ctx.awaitExecute("inner", ...); // appends a task from inside a task consumer → blocks forever
 *     return Map.of();
 * });
 * }</pre>
 *
 * <p>It exists ONLY as a PROBE for INV-23 ({@code EngineSelfProtection}) — to observe, end-to-end, what the running
 * engine actually does at this self-protection surface. It is NOT a model of correct authoring and is never folded into
 * the always-on fuzz set (a stuck instance would deliberately stall non-terminally, which the liveness horizon check
 * would read as a hang); it is driven only by the scenario-pinned {@code EngineSelfProtectionScenario} under a SHORT
 * wall-clock window so the deadlock is observed as non-terminal within ~1s and the test finishes fast.
 *
 * <p>Why this deadlocks rather than being rejected (the engine mapping): the runtime drives each instance through a
 * single-threaded per-instance task queue ({@code SimpleWorkflowExecution.taskQueue}, an {@code ArrayBlockingQueue}).
 * The outer step's action runs on a body thread while {@code AbstractStepExecutor.acceptAllPendingTasksForStep} /
 * {@code SimpleWorkflowExecution.awaitStateChange} is the single consumer of that queue. The inner
 * {@code ctx.awaitExecute(...)} appends its STARTED task to the SAME queue and then blocks the outer action waiting for
 * that task to be consumed and the inner step to reach terminal — but the consumer cannot make progress because it is
 * busy running the outer task that is blocked on the inner one. Nothing detects the nesting up front (there is no
 * re-entrancy guard on {@code appendTask}), so the per-instance queue deadlocks: the instance makes no further progress
 * and reaches NO terminal status. The harness's bounded wall-clock deadline catches it as a non-terminal stuck instance
 * (the documented robustness gap INV-23's nested-primitive probe surfaces).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class NestedPrimitiveWorkflow {

    /**
     * Logical workflow name, reused by the harness when registering the definition.
     */
    public static final String WORKFLOW_NAME = "NestedPrimitiveWorkflow";

    /**
     * The OUTER step whose action lambda forbidden-ly nests an inner primitive. Its STARTED event is the only step event
     * that ever lands in the committed log — the outer step never COMPLETES because its action blocks on the nested
     * inner call forever (the per-instance queue deadlock).
     */
    public static final String STEP_OUTER = "outerStep";

    /**
     * The INNER step the outer action illegally calls {@code ctx.awaitExecute} for. Because the nested call blocks the
     * single-threaded consumer, this step's STARTED task is appended but never consumed — so {@link #STEP_INNER}
     * produces NO committed event (no torn/half-written record).
     */
    public static final String STEP_INNER = "innerStep";

    private final CountingEffects effects;

    /**
     * Creates the probe workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes; the (unreachable-completion) action bodies bump a counter here so
     *                the scenario can confirm the inner action never ran.
     */
    public NestedPrimitiveWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * Workflow body that performs the §3.1-forbidden nested-primitive call: the outer {@code awaitExecute}'s action
     * lambda itself calls {@code ctx.awaitExecute(...)}. See the class Javadoc for why this deadlocks the per-instance
     * task queue rather than being rejected up front.
     *
     * @param ctx the workflow context provided by the runtime.
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();

        ctx.awaitExecute(
                STEP_OUTER,
                Map.of(),
                (pc, payload) -> {
                    // ✗ FORBIDDEN nested primitive — calling a primitive from inside another primitive's action lambda.
                    // This appends the inner STARTED task to the SAME single-threaded per-instance queue and then blocks
                    // this outer action waiting for the inner step to terminate; the consumer is THIS thread (busy here),
                    // so the inner task is never consumed → deadlock. The line below therefore never returns.
                    ctx.awaitExecute(
                            STEP_INNER,
                            Integer.class,
                            () -> effects.record(workflowId, STEP_INNER) // never runs — the inner task is never consumed
                    );
                    // Unreachable — the nested call above blocks forever.
                    return Map.of("outer", effects.record(workflowId, STEP_OUTER));
                }
        );
    }
}
