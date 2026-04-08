/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventhandling.processing.errorhandling;

import org.axonframework.messaging.eventhandling.processing.EventProcessingException;

/**
 * An {@link ErrorHandler} implementation that rethrows the {@link ErrorContext#error() ErrorContext exception}.
 *
 * @author Rene de Waele
 * @since 3.0.0
 */
public enum PropagatingErrorHandler implements ErrorHandler {

    /**
     * Singleton instance of a {@link PropagatingErrorHandler}.
     */
    INSTANCE;

    /**
     * Singleton instance of a {@code PropagatingErrorHandler}.
     *
     * @return The singleton instance of {@code PropagatingErrorHandler}
     */
    public static PropagatingErrorHandler instance() {
        return INSTANCE;
    }

    @Override
    public void handleError(ErrorContext errorContext) throws Exception {
        Throwable error = errorContext.error();
        if (error instanceof Error) {
            throw (Error) error;
        } else if (error instanceof Exception) {
            throw (Exception) error;
        } else {
            throw new EventProcessingException("An error occurred while handling an event.", error);
        }
    }
}
