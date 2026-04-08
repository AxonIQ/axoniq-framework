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

package org.axonframework.messaging.eventhandling.processing.streaming.token.store;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating that a TokenStore implementation was unable determine its identifier based on the underlying
 * storage.
 *
 * @author Allard Buijze
 * @see TokenStore#retrieveStorageIdentifier(org.axonframework.messaging.core.unitofwork.ProcessingContext)
 * @since 4.3
 */
public class UnableToRetrieveIdentifierException extends AxonTransientException {

    /**
     * Initialize the exception using given {@code message} and {@code cause}.
     *
     * @param message A message describing the exception
     * @param cause   The underlying cause of the exception
     */
    public UnableToRetrieveIdentifierException(String message, Throwable cause) {
        super(message, cause);
    }
}
