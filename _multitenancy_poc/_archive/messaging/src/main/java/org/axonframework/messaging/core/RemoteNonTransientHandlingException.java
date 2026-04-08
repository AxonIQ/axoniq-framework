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

package org.axonframework.messaging.core;

import org.axonframework.common.AxonNonTransientException;

import java.util.List;

/**
 * Exception indicating that a non transient error has occurred while remotely handling a message.
 * <p/>
 * The sender of the message <strong>cannot</strong> assume that the message has not been handled.
 *
 * @author Stefan Andjelkovic
 * @since 4.5
 */
public class RemoteNonTransientHandlingException extends AxonNonTransientException {

    private final List<String> exceptionDescriptions;

    /**
     * Initializes the exception using the given {@code exceptionDescription} describing the remote cause-chain.
     *
     * @param exceptionDescription a {@link String} describing the remote exceptions
     */
    public RemoteNonTransientHandlingException(RemoteExceptionDescription exceptionDescription) {
        super("An exception was thrown by the remote message handling component: " + exceptionDescription.toString());
        this.exceptionDescriptions = exceptionDescription.getDescriptions();
    }

    /**
     * Returns a {@link List} of {@link String}s describing the remote exception.
     *
     * @return a {@link List} of {@link String}s describing the remote exception
     */
    public List<String> getExceptionDescriptions() {
        return exceptionDescriptions;
    }
}
