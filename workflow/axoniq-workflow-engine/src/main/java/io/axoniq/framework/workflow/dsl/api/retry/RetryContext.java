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


/**
 * Context passed to a {@link RetryHandler} on each retry attempt.
 *
 * @param stepName   name of the step being retried.
 * @param attempt    current retry attempt number (1-based).
 * @param maxRetries maximum number of retries configured.
 * @param error      the error that triggered the retry.
 * @param delay      the computed backoff delay before this retry attempt.
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record RetryContext(String stepName, int attempt, int maxRetries, Throwable error,
                           java.time.Duration delay) {

    /**
     * Creates a retry context with no backoff delay.
     *
     * @param stepName   name of the step being retried
     * @param attempt    current retry attempt number (1-based)
     * @param maxRetries maximum number of configured retries
     * @param error      error that triggered the retry
     */
    public RetryContext(String stepName, int attempt, int maxRetries, Throwable error) {
        this(stepName, attempt, maxRetries, error, java.time.Duration.ZERO);
    }
}
