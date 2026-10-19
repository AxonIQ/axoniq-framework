/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


/**
 * Exception thrown into the workflow body when a step's blocking wait is interrupted because the
 * <em>workflow itself</em> reached a terminal state (cancelled, failed, timed out) or the engine is shutting down.
 * <p>
 * A subtype of {@link StepFailedException}: callers that want to handle any step-level failure can catch the parent;
 * callers that need to distinguish this specific case can catch this type. Distinct from
 * {@link StepCancellationException}, which is a per-step cancellation with its own durable {@code <step>:CANCELLED}
 * record.
 * <p>
 * No durable event backs this exception: the step's last recorded event-log state stays {@code STARTED}. This is purely
 * an in-body signal so the workflow body can run compensation or cleanup logic around its blocking wait; it carries no
 * bearing on what gets persisted.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class StepInterruptedException extends StepFailedException {

    /**
     * Constructs a {@code StepInterruptedException} with a descriptive message.
     *
     * @param message the detail message describing why the wait was interrupted
     */
    public StepInterruptedException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code StepInterruptedException} with a message and underlying cause.
     *
     * @param message the detail message describing why the wait was interrupted
     * @param cause   the underlying cause of the interruption
     */
    public StepInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a {@code StepInterruptedException} wrapping an underlying cause.
     *
     * @param cause the underlying cause of the interruption
     */
    public StepInterruptedException(Throwable cause) {
        super(cause);
    }
}
