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
package io.axoniq.workflow.runtime.api;

/**
 * Retry policy for execute steps. {@code maxRetries} is the number of retry attempts
 * (not counting the initial attempt). {@code maxRetries=3} means up to 4 total executions.
 *
 * @param maxRetries     maximum number of retry attempts.
 * @param onRetryHandler handler invoked before each retry event is published.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public record RetryPolicy(int maxRetries, RetryHandler onRetryHandler, BackoffStrategy backoffStrategy) {

    public RetryPolicy(int maxRetries, RetryHandler onRetryHandler) {
        this(maxRetries, onRetryHandler, BackoffStrategy.NONE);
    }

    public static final RetryPolicy NONE = new RetryPolicy(0, RetryHandler.NOOP);

    public static RetryPolicy maxRetries(int maxRetries) {
        return new RetryPolicy(maxRetries, RetryHandler.NOOP);
    }

    public RetryPolicy onRetry(RetryHandler handler) {
        return new RetryPolicy(this.maxRetries, handler, this.backoffStrategy);
    }

    public RetryPolicy withBackoff(BackoffStrategy strategy) {
        return new RetryPolicy(this.maxRetries, this.onRetryHandler, strategy);
    }

    public boolean shouldRetry(int currentAttempt) {
        return currentAttempt < maxRetries;
    }
}
