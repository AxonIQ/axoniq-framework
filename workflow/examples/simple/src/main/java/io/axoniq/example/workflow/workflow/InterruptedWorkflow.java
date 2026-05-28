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
package io.axoniq.example.workflow.workflow;

import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Workflow that executes a step, then interrupts the current thread before executing the next step. Used to verify that
 * an {@link InterruptedException} does not put the workflow into a terminal state.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class InterruptedWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(InterruptedWorkflow.class);

    public void execute(@Nonnull BaseWorkflowContext ctx) {
        logger.info("Interrupted workflow started for {}", ctx.workflowPayload());

        ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

        // Simulate thread interruption (e.g. JVM shutdown, executor interruption)
        logger.info("Interrupting workflow thread");
        Thread.currentThread().interrupt();

        ctx.awaitExecute("stepB", Map.of(), (c, p) -> Map.of("result", "done"));
    }
}
