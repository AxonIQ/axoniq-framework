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
import jakarta.annotation.Nullable;

/**
 * Base exception type for individual step failures surfaced by the DSL.
 * <p>
 * Catch {@code StepFailedException} to handle any abnormal step termination — failure,
 * timeout, or cancellation. {@link StepTimedOutException} and {@link StepCancellationException}
 * are subtypes for callers that need to distinguish those cases.
 */
public class StepFailedException extends RuntimeException {

    public StepFailedException(@Nonnull String message) {
        super(message);
    }

    public StepFailedException(@Nonnull Throwable cause) {
        super(cause);
    }

    public StepFailedException(@Nonnull String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
