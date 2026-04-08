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

package org.axonframework.modelling.saga;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating that an error has occurred while storing a Saga.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SagaStorageException extends AxonTransientException {

    /**
     * Initialize a SagaStorageException with the given descriptive {@code message}.
     *
     * @param message The message describing the error.
     */
    public SagaStorageException(String message) {
        super(message);
    }

    /**
     * Initialize a SagaStorageException with the given descriptive {@code message} and {@code cause}.
     *
     * @param message The message describing the error.
     * @param cause   The cause of the error.
     */
    public SagaStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
