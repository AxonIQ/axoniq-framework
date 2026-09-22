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

import org.jspecify.annotations.Nullable;

/**
 * Base exception type for individual step failures surfaced by the DSL.
 * <p>
 * Catch {@code StepFailedException} to handle any abnormal step termination — failure,
 * timeout, or cancellation. {@link StepTimedOutException} and {@link StepCancellationException}
 * are subtypes for callers that need to distinguish those cases.
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
