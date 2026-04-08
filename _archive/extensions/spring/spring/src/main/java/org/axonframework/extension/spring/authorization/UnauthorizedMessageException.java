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

package org.axonframework.extension.spring.authorization;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception indicating that a message has been rejected due to a lack of authorization.
 *
 * @author Roald Bankras
 * @since 4.11.0
 */
public class UnauthorizedMessageException extends AxonNonTransientException {

    /**
     * Construct the exception with the given {$code message}.
     *
     * @param message The message describing the cause.
     */
    public UnauthorizedMessageException(String message) {
        super(message);
    }
}
