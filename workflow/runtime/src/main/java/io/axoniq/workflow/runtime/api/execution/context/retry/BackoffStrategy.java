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
package io.axoniq.workflow.runtime.api.execution.context.retry;

import java.time.Duration;

/**
 * Strategy for computing delay between retry attempts.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@FunctionalInterface
public interface BackoffStrategy {

    BackoffStrategy NONE = attempt -> Duration.ZERO;

    /**
     * Computes the delay before the given retry attempt.
     *
     * @param attempt the retry attempt number (1-based).
     * @return the duration to wait before executing the attempt.
     */
    Duration delay(int attempt);

    /**
     * Fixed delay between every retry attempt.
     *
     * @param delay constant delay duration.
     * @return a backoff strategy with fixed delay.
     */
    static BackoffStrategy fixed(Duration delay) {
        return attempt -> delay;
    }

    /**
     * Linear backoff: {@code baseDelay * attempt}.
     *
     * @param baseDelay base delay multiplied by the attempt number.
     * @return a backoff strategy with linearly increasing delay.
     */
    static BackoffStrategy linear(Duration baseDelay) {
        return attempt -> baseDelay.multipliedBy(attempt);
    }

    /**
     * Exponential backoff: {@code min(base * 2^(attempt-1), max)}.
     *
     * @param base base delay for the first attempt.
     * @param max  maximum delay cap.
     * @return a backoff strategy with exponentially increasing delay.
     */
    static BackoffStrategy exponential(Duration base, Duration max) {
        return attempt -> {
            long factor = 1L << (attempt - 1); // 2^(attempt-1)
            Duration computed = base.multipliedBy(factor);
            return computed.compareTo(max) > 0 ? max : computed;
        };
    }
}
