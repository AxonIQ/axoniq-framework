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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * A workflow whose body throws an {@link UncheckedIOException} wrapping an {@link IOException} between its two steps,
 * on its first run only.
 * <p>
 * The default
 * {@link io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy} classifies an
 * {@link IOException} anywhere in the cause chain as recoverable, so the first run pauses the instance non-terminally.
 * The next run, after a restart or a claim, replays the first step from history, passes the check and completes.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class FlakyBodyWorkflow {

    /**
     * Logical workflow name.
     */
    public static final String WORKFLOW_NAME = "FlakyBodyWorkflow";

    /**
     * First step: a counting side effect that must run exactly once across the pause and the re-drive.
     */
    public static final String STEP_RESERVE = "reserveInventory";

    /**
     * Effect key counting how often the body reached the point between the two steps.
     */
    public static final String BODY_CHECK = "bodyCheck";

    /**
     * Second step: a counting side effect that runs once, on the re-drive.
     */
    public static final String STEP_FULFILL = "fulfillOrder";

    private final CountingEffects effects;

    /**
     * Creates the workflow bound to the given effect registry.
     *
     * @param effects registry that survives crashes and restarts
     */
    public FlakyBodyWorkflow(CountingEffects effects) {
        this.effects = effects;
    }

    /**
     * The body: reserve, throw a transient I/O failure on the first pass only, then fulfill.
     *
     * @param ctx the workflow context provided by the runtime
     */
    public void execute(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        ctx.awaitExecute(STEP_RESERVE, Map.of(),
                         (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE)));
        if (effects.record(workflowId, BODY_CHECK) == 1) {
            throw new UncheckedIOException(new IOException("the inventory service is unreachable"));
        }
        ctx.awaitExecute(STEP_FULFILL, Map.of(),
                         (pc, payload) -> Map.of("fulfilled", effects.record(workflowId, STEP_FULFILL)));
    }

    /**
     * A body that fails the workflow on any step failure it sees, the pattern many authors write: reserve, then
     * fulfill, and {@code ctx.fail(e)} on every {@link StepFailedException}. It does not single out an engine interrupt,
     * so it shows whether the engine reports an interrupted step start as a failure.
     *
     * @param ctx the workflow context provided by the runtime
     */
    public void executeFailingOnAnyStepFailure(SimpleWorkflowContext ctx) {
        String workflowId = ctx.workflowId();
        try {
            ctx.awaitExecute(STEP_RESERVE, Map.of(),
                             (pc, payload) -> Map.of("reserved", effects.record(workflowId, STEP_RESERVE)));
            ctx.awaitExecute(STEP_FULFILL, Map.of(),
                             (pc, payload) -> Map.of("fulfilled", effects.record(workflowId, STEP_FULFILL)));
        } catch (StepFailedException e) {
            ctx.fail(e);
        }
    }
}
