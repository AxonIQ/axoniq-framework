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
package io.axoniq.workflow.runtime.api.execution.state;


/**
 * Exception thrown when an individual step is cancelled.
 * <p>
 * A subtype of {@link StepFailedException}: callers that want to handle any step-level
 * failure can catch the parent; callers that need to distinguish a cancellation specifically
 * can catch this type. Distinct from {@link StepTimedOutException} (timeout).
 *
 * @author Stefan Dragisic
 * @since 0.1.0
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
