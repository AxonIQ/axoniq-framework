/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api.retry;

import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryContextTest {

    @Test
    void testRetryContext() {
        String stepName = "testStep";
        int attempt = 2;
        int maxRetries = 5;
        Throwable error = new RuntimeException("Test error");
        Duration delay = Duration.ofSeconds(10);

        RetryContext context = new RetryContext(stepName, attempt, maxRetries, error, delay);

        assertThat(context.stepName()).isEqualTo(stepName);
        assertThat(context.attempt()).isEqualTo(attempt);
        assertThat(context.maxRetries()).isEqualTo(maxRetries);
        assertThat(context.error()).isEqualTo(error);
        assertThat(context.delay()).isEqualTo(delay);
    }

    @Test
    void testRetryContextDefaultDelay() {
        String stepName = "testStep";
        int attempt = 1;
        int maxRetries = 3;
        Throwable error = new RuntimeException("Test error");

        RetryContext context = new RetryContext(stepName, attempt, maxRetries, error);

        assertThat(context.stepName()).isEqualTo(stepName);
        assertThat(context.attempt()).isEqualTo(attempt);
        assertThat(context.maxRetries()).isEqualTo(maxRetries);
        assertThat(context.error()).isEqualTo(error);
        assertThat(context.delay()).isEqualTo(Duration.ZERO);
    }
}
