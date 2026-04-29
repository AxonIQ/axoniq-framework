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
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.example.workflow.fixture.SleepUtils.sleepQuietly;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class CancelWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(CancelWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("Cancel workflow started for {}", ctx.workflowPayload());

        // Launch 3 long-running steps (non-blocking, each simulates 5 min work)
        Duration fiveMin = Duration.ofMinutes(5);
        long fiveMinMs = fiveMin.toMillis();
        var r1 = ctx.execute("stepA", Map.of(), (c, p) -> {
            sleepQuietly(fiveMinMs);
            return Map.of();
        }, fiveMin, defaults());
        var r2 = ctx.execute("stepB", Map.of(), (c, p) -> {
            sleepQuietly(fiveMinMs);
            return Map.of();
        }, fiveMin, defaults());
        var r3 = ctx.execute("stepC", Map.of(), (c, p) -> {
            sleepQuietly(fiveMinMs);
            return Map.of();
        }, fiveMin, defaults());

        // Combine results but don't block on them
        ctx.allMatch(WorkflowStepResult::isCompleted, r1, r2, r3);

        // Wait 5 seconds then cancel
        sleepQuietly(5_000);
        logger.info("Cancelling workflow after 5 seconds");
        ctx.cancel("I dont want it anymore");
    }
}
