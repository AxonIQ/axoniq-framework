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
package io.axoniq.workflow.runtime.api.execution.retry;

import io.axoniq.workflow.runtime.api.execution.context.retry.RetryContext;
import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class RetryContextTest {

    @Test
    void testRetryContext() {
        String stepName = "testStep";
        int attempt = 2;
        int maxRetries = 5;
        Throwable error = new RuntimeException("Test error");
        Duration delay = Duration.ofSeconds(10);

        RetryContext context = new RetryContext(stepName, attempt, maxRetries, error, delay);

        assertEquals(stepName, context.stepName());
        assertEquals(attempt, context.attempt());
        assertEquals(maxRetries, context.maxRetries());
        assertEquals(error, context.error());
        assertEquals(delay, context.delay());
    }

    @Test
    void testRetryContextDefaultDelay() {
        String stepName = "testStep";
        int attempt = 1;
        int maxRetries = 3;
        Throwable error = new RuntimeException("Test error");

        RetryContext context = new RetryContext(stepName, attempt, maxRetries, error);

        assertEquals(stepName, context.stepName());
        assertEquals(attempt, context.attempt());
        assertEquals(maxRetries, context.maxRetries());
        assertEquals(error, context.error());
        assertEquals(Duration.ZERO, context.delay());
    }
}
