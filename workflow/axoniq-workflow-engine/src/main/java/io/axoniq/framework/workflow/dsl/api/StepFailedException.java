/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;

/**
 * Base exception type for individual step failures surfaced by the DSL.
 * <p>
 * Catch {@code StepFailedException} to handle any abnormal step termination — failure, timeout, or cancellation.
 * {@link StepTimedOutException} and {@link StepCancellationException} are subtypes for callers that need to distinguish
 * those cases.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class StepFailedException extends RuntimeException {

    /**
     * Constructs a {@code StepFailedException} with a descriptive message.
     *
     * @param message the detail message describing why the step failed
     */
    public StepFailedException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code StepFailedException} wrapping an underlying cause.
     *
     * @param cause the underlying cause of the failure
     */
    public StepFailedException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a {@code StepFailedException} with a message and an optional underlying cause.
     *
     * @param message the detail message describing why the step failed
     * @param cause   the underlying cause of the failure, or {@code null} if none
     */
    public StepFailedException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
