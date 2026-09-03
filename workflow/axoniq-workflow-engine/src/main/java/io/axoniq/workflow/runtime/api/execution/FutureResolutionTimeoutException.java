/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.api.execution;

import io.axoniq.workflow.runtime.util.FutureResolver;
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
