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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.declarative;

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class CancelWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(CancelWorkflow.class);

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("Cancel workflow started for {}", ctx.getPayload());

        // Launch 3 long-running steps (non-blocking, each simulates 5 min work)
        Duration fiveMin = Duration.ofMinutes(5);
        long fiveMinMs = fiveMin.toMillis();
        var r1 = ctx.execute("stepA", Map.of(), (c, p) -> {
            sleepQuietly(fiveMinMs);
            return Map.of();
        }, fiveMin);
        var r2 = ctx.execute("stepB", Map.of(), (c, p) -> {
            sleepQuietly(fiveMinMs);
            return Map.of();
        }, fiveMin);
        var r3 = ctx.execute("stepC", Map.of(), (c, p) -> {
            sleepQuietly(fiveMinMs);
            return Map.of();
        }, fiveMin);

        // Combine results but don't block on them
        ctx.all(r1, r2, r3);

        // Wait 5 seconds then cancel
        sleepQuietly(5_000);
        logger.info("Cancelling workflow after 5 seconds");
        ctx.cancel("I dont want it anymore");
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
