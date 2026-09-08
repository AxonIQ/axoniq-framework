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
package io.axoniq.framework.workflow.runtime.api.execution.state;


/**
 * Exception thrown into the workflow body when a step's blocking wait is interrupted because the
 * <em>workflow itself</em> reached a terminal state (cancelled, failed, timed out) or the engine is shutting down.
 * <p>
 * A subtype of {@link StepFailedException}: callers that want to handle any step-level failure can catch the
 * parent; callers that need to distinguish this specific case can catch this type. Distinct from
 * {@link StepCancellationException}, which is a per-step cancellation with its own durable
 * {@code <step>:CANCELLED} record.
 * <p>
 * No durable event backs this exception: the step's last recorded event-log state stays {@code STARTED}. This is
 * purely an in-body signal so the workflow body can run compensation or cleanup logic around its blocking wait; it
 * carries no bearing on what gets persisted.
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
