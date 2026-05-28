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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Workflow that calls {@code ctx.fail()}, catches the exception, and attempts to execute another step. The guard should
 * prevent the second step from executing.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class FailWithCatchWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(FailWithCatchWorkflow.class);

    public void execute(@Nonnull BaseWorkflowContext ctx) {
        logger.info("FailWithCatch workflow started for {}", ctx.workflowPayload());

        ctx.awaitExecute("stepA", Map.of(), (c, p) -> Map.of("result", "done"));

        try {
            ctx.fail(new RuntimeException("Simulated failure"));
        } catch (WorkflowFailedException e) {
            logger.info("Caught WorkflowFailedException, attempting another step...");
            try {
                // This should rethrow the original error because workflow is in terminal state
                ctx.awaitExecute("stepAfterFail", Map.of(), (c, p) -> Map.of("result", "should not happen"));
            } catch (WorkflowFailedException e2) {
                logger.info("Guard correctly prevented step execution after fail: {}", e2.getMessage());
            }
        }
    }
}
