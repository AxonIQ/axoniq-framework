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
package io.axoniq.workflow.runtime.api.execution.context.retry;


import java.util.function.Predicate;

/**
 * Retry policy for execute steps. {@code maxRetries} is the number of retry attempts
 * (not counting the initial attempt). {@code maxRetries=3} means up to 4 total executions.
 *
 * @param maxRetries     maximum number of retry attempts.
 * @param onRetryHandler handler invoked before each retry event is published.
 * @param backoffStrategy backoff strategy between retry attempts.
 * @param retryPredicate predicate evaluated on each retry; returns {@code true} to continue retrying,
 *                       {@code false} to stop. Checked in addition to {@code maxRetries}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record RetryPolicy(int maxRetries, RetryHandler onRetryHandler,
                           BackoffStrategy backoffStrategy,
                           Predicate<RetryContext> retryPredicate) {

    /**
     * Creates a retry policy with explicit retry count, retry hook, and backoff strategy.
     *
     * @param maxRetries maximum number of retry attempts
     * @param onRetryHandler handler invoked before each retry event is published
     * @param backoffStrategy backoff strategy between retry attempts
     */
    public RetryPolicy(int maxRetries, RetryHandler onRetryHandler,
                       BackoffStrategy backoffStrategy) {
        this(maxRetries, onRetryHandler, backoffStrategy, ctx -> true);
    }

    /**
     * Creates a retry policy with explicit retry count and retry hook, using no backoff.
     *
     * @param maxRetries maximum number of retry attempts
     * @param onRetryHandler handler invoked before each retry event is published
     */
    public RetryPolicy(int maxRetries, RetryHandler onRetryHandler) {
        this(maxRetries, onRetryHandler, BackoffStrategy.NONE, ctx -> true);
    }

    /**
     * Constant retry policy that disables retries entirely.
     */
    public static final RetryPolicy NONE = new RetryPolicy(0, RetryHandler.NOOP);

    /**
     * Creates a retry policy with the provided retry count and no-op retry handling.
     *
     * @param maxRetries maximum number of retry attempts
     * @return retry policy with the provided retry count
     */
    public static RetryPolicy maxRetries(int maxRetries) {
        return new RetryPolicy(maxRetries, RetryHandler.NOOP);
    }

    /**
     * Returns a copy of this retry policy with the provided retry handler.
     *
     * @param handler handler invoked before each retry event is published
     * @return copied retry policy with updated retry handler
     */
    public RetryPolicy onRetry(RetryHandler handler) {
        return new RetryPolicy(this.maxRetries, handler, this.backoffStrategy, this.retryPredicate);
    }

    /**
     * Returns a copy of this retry policy with the provided backoff strategy.
     *
     * @param strategy backoff strategy between retry attempts
     * @return copied retry policy with updated backoff strategy
     */
    public RetryPolicy withBackoff(BackoffStrategy strategy) {
        return new RetryPolicy(this.maxRetries, this.onRetryHandler, strategy, this.retryPredicate);
    }

    /**
     * Returns a copy of this retry policy with the provided retry predicate.
     *
     * @param predicate predicate controlling whether retrying may continue
     * @return copied retry policy with updated retry predicate
     */
    public RetryPolicy retryWhile(Predicate<RetryContext> predicate) {
        return new RetryPolicy(this.maxRetries, this.onRetryHandler, this.backoffStrategy, predicate);
    }

    /**
     * Evaluates whether the supplied retry context permits another retry attempt.
     *
     * @param context retry context for the current failure
     * @return {@code true} when retrying should continue, otherwise {@code false}
     */
    public boolean shouldRetry(RetryContext context) {
        return context.attempt() <= maxRetries && retryPredicate.test(context);
    }
}
