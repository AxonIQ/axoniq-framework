/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
