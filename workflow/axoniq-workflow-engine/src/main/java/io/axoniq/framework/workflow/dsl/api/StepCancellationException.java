/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


/**
 * Exception thrown when an individual step is cancelled.
 * <p>
 * A subtype of {@link StepFailedException}: callers that want to handle any step-level failure can catch the parent;
 * callers that need to distinguish a cancellation specifically can catch this type. Distinct from
 * {@link StepTimedOutException} (timeout).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class StepCancellationException extends StepFailedException {

    /**
     * Constructs a {@code StepCancellationException} with a descriptive message.
     *
     * @param message the detail message describing the cancellation reason
     */
    public StepCancellationException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code StepCancellationException} with a message and underlying cause.
     *
     * @param message the detail message describing the cancellation reason
     * @param cause   the underlying cause of the cancellation
     */
    public StepCancellationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a {@code StepCancellationException} wrapping an underlying cause.
     *
     * @param cause the underlying cause of the cancellation
     */
    public StepCancellationException(Throwable cause) {
        super(cause);
    }
}
