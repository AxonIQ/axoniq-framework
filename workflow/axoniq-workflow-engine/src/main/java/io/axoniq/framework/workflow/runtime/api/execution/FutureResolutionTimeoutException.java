/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution;

import io.axoniq.framework.workflow.runtime.util.FutureResolver;

import java.util.concurrent.TimeoutException;

/**
 * Indicates that a {@link FutureResolver} stopped waiting before an asynchronous operation completed.
 * <p>
 * This is distinct from {@link TimeoutException}, which workflow execution uses to represent a durable workflow
 * timeout.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class FutureResolutionTimeoutException extends RuntimeException {

    /**
     * Constructs an exception for an expired future-resolution timeout.
     *
     * @param cause timeout reported by the future-resolution operation
     */
    public FutureResolutionTimeoutException(TimeoutException cause) {
        super("Future did not complete before the configured resolution timeout", cause);
    }
}
