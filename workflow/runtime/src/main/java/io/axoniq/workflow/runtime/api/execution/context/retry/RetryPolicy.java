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

import jakarta.annotation.Nonnull;

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
 * @since 1.0.0
 */
public record RetryPolicy(int maxRetries, @Nonnull RetryHandler onRetryHandler,
                           @Nonnull BackoffStrategy backoffStrategy,
                           @Nonnull Predicate<RetryContext> retryPredicate) {

    public RetryPolicy(int maxRetries, @Nonnull RetryHandler onRetryHandler,
                       @Nonnull BackoffStrategy backoffStrategy) {
        this(maxRetries, onRetryHandler, backoffStrategy, ctx -> true);
    }

    public RetryPolicy(int maxRetries, @Nonnull RetryHandler onRetryHandler) {
        this(maxRetries, onRetryHandler, BackoffStrategy.NONE, ctx -> true);
    }

    public static final RetryPolicy NONE = new RetryPolicy(0, RetryHandler.NOOP);

    @Nonnull
    public static RetryPolicy maxRetries(int maxRetries) {
        return new RetryPolicy(maxRetries, RetryHandler.NOOP);
    }

    @Nonnull
    public RetryPolicy onRetry(@Nonnull RetryHandler handler) {
        return new RetryPolicy(this.maxRetries, handler, this.backoffStrategy, this.retryPredicate);
    }

    @Nonnull
    public RetryPolicy withBackoff(@Nonnull BackoffStrategy strategy) {
        return new RetryPolicy(this.maxRetries, this.onRetryHandler, strategy, this.retryPredicate);
    }

    @Nonnull
    public RetryPolicy retryWhile(@Nonnull Predicate<RetryContext> predicate) {
        return new RetryPolicy(this.maxRetries, this.onRetryHandler, this.backoffStrategy, predicate);
    }

    public boolean shouldRetry(@Nonnull RetryContext context) {
        return context.attempt() <= maxRetries && retryPredicate.test(context);
    }
}
