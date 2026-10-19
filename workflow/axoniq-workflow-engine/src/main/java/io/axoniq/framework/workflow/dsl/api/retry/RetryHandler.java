/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api.retry;

/**
 * Callback invoked on each retry attempt, before publishing the RETRYING event. Use for logging, metrics, or other side
 * effects. Not called during replay.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface RetryHandler {

    /**
     * Retry handler that performs no side effects.
     */
    RetryHandler NOOP = context -> {
    };

    /**
     * Invoked when a retry attempt is about to be published and executed.
     *
     * @param context retry context describing the step, attempt, and triggering failure
     */
    void onRetry(RetryContext context);
}
