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

import jakarta.annotation.Nonnull;

/**
 * Exception thrown when an individual step reaches its configured timeout before completing.
 * <p>
 * Surfaced by DSL helpers (e.g. {@code awaitEvent}) when the awaited event does not arrive within
 * the timeout and the step ends in the {@code TIMED_OUT} state. Distinct from
 * {@link StepFailedException} (caller-supplied failure) and {@link StepCancellationException}
 * (explicit cancellation).
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class StepTimedOutException extends RuntimeException {

    /**
     * Constructs a {@code StepTimedOutException} with a descriptive message.
     *
     * @param message the detail message describing which step timed out and how long it waited
     */
    public StepTimedOutException(@Nonnull String message) {
        super(message);
    }

    /**
     * Constructs a {@code StepTimedOutException} with a message and underlying cause.
     *
     * @param message the detail message describing which step timed out
     * @param cause   the underlying cause of the timeout, if any
     */
    public StepTimedOutException(@Nonnull String message, Throwable cause) {
        super(message, cause);
    }
}
