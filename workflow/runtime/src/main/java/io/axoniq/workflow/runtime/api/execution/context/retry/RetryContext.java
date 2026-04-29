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

/**
 * Context passed to a {@link RetryHandler} on each retry attempt.
 *
 * @param stepName   name of the step being retried.
 * @param attempt    current retry attempt number (1-based).
 * @param maxRetries maximum number of retries configured.
 * @param error      the error that triggered the retry.
 * @param delay      the computed backoff delay before this retry attempt.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public record RetryContext(@Nonnull String stepName, int attempt, int maxRetries, @Nonnull Throwable error,
                           @Nonnull java.time.Duration delay) {

    public RetryContext(@Nonnull String stepName, int attempt, int maxRetries, @Nonnull Throwable error) {
        this(stepName, attempt, maxRetries, error, java.time.Duration.ZERO);
    }
}
