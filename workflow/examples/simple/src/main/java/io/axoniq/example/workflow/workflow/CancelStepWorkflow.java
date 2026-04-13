/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.example.workflow.workflow;

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.example.workflow.fixture.SleepUtils.sleepQuietly;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Example workflow that launches three parallel steps, cancels one by name, then completes normally. Used to verify
 * single-step cancellation via {@code cancelStep}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class CancelStepWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(CancelStepWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("CancelStep workflow started for {}", ctx.workflowPayload());

        Duration fiveMin = Duration.ofMinutes(5);
        long fiveMinMs = fiveMin.toMillis();

        // Launch 3 long-running parallel steps
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

        // Cancel only stepB via the result handle
        sleepQuietly(1_000);
        logger.info("Cancelling stepB");
        r2.cancel("No longer needed");

        //finish
        sleepQuietly(1_000);
    }
}
