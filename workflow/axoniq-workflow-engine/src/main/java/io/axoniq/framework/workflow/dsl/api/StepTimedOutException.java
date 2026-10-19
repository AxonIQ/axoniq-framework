/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


/**
 * Exception thrown when an individual step reaches its configured timeout before completing.
 * <p>
 * Surfaced by DSL helpers (e.g. {@code awaitEvent}, {@code awaitExecute}) when the step ends in the {@code TIMED_OUT}
 * state. A subtype of {@link StepFailedException}: callers that want to handle any step-level failure can catch the
 * parent; callers that need to distinguish a timeout specifically can catch this type. Distinct from
 * {@link StepCancellationException} (explicit cancellation).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class StepTimedOutException extends StepFailedException {

    /**
     * Constructs a {@code StepTimedOutException} with a descriptive message.
     *
     * @param message the detail message describing which step timed out and how long it waited
     */
    public StepTimedOutException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code StepTimedOutException} with a message and underlying cause.
     *
     * @param message the detail message describing which step timed out
     * @param cause   the underlying cause of the timeout, if any
     */
    public StepTimedOutException(String message, Throwable cause) {
        super(message, cause);
    }
}
