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
package io.axoniq.framework.workflow.dsl.api.retry;

import java.time.Duration;

/**
 * Strategy for computing delay between retry attempts.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface BackoffStrategy {

    /**
     * Backoff strategy that applies no delay between retry attempts.
     */
    BackoffStrategy NONE = attempt -> Duration.ZERO;

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
     * Exponential backoff: {@code min(base * 2^(attempt-1), max)}, clamped to {@code max} at large attempt counts so
     * the factor/multiply can never overflow, go negative, or wrap.
     *
     * @param base base delay for the first attempt.
     * @param max  maximum delay cap.
     * @return a backoff strategy with exponentially increasing delay.
     */
    static BackoffStrategy exponential(Duration base, Duration max) {
        return attempt -> {
            // Clamp to max before any overflowing/negative/wrapped shift or multiply.
            long factor = 1L << Math.min(Math.max(attempt - 1, 0), 62);
            if (base.isZero() || base.isNegative() || factor > max.dividedBy(base)) {
                return max;
            }
            Duration computed = base.multipliedBy(factor);
            return computed.compareTo(max) > 0 ? max : computed;
        };
    }

    /**
     * Computes the delay before the given retry attempt.
     *
     * @param attempt the retry attempt number (1-based).
     * @return the duration to wait before executing the attempt.
     */
    Duration delay(int attempt);
}
