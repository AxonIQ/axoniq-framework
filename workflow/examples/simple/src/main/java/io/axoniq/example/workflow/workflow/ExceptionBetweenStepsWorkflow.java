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

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Workflow that executes a step, then throws a {@link RuntimeException} in user code between steps.
 * Used to verify that an exception thrown between steps does not put the workflow into a terminal state.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class ExceptionBetweenStepsWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionBetweenStepsWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("ExceptionBetweenSteps workflow started for {}", ctx.workflowPayload());

        ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

        // Simulate an error in user code between steps
        logger.info("Throwing exception between steps");
        riskyComputation();

        ctx.awaitExecute("stepB", Map.of(), (c, p) -> Map.of("result", "done"));
    }

    private void riskyComputation() {
        throw new RuntimeException("Unexpected error between steps");
    }
}
