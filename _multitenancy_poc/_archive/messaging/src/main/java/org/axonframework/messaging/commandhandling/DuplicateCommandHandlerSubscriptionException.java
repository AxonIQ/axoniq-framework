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

package org.axonframework.messaging.commandhandling;

import org.axonframework.messaging.core.QualifiedName;

/**
 * Exception indicating a duplicate {@link CommandHandler} was subscribed.
 *
 * @author Steven van Beelen
 * @since 4.2.0
 */
public class DuplicateCommandHandlerSubscriptionException extends RuntimeException {

    /**
     * Initialize a duplicate {@link CommandHandler} subscription exception using the given {@code initialHandler} and
     * {@code duplicateHandler} to form a specific message.
     *
     * @param name             The name of the command for which the duplicate was detected.
     * @param initialHandler   the initial {@link CommandHandler} for which a duplicate was encountered.
     * @param duplicateHandler The duplicated {@link CommandHandler}.
     */
    public DuplicateCommandHandlerSubscriptionException(QualifiedName name,
                                                        CommandHandler initialHandler,
                                                        CommandHandler duplicateHandler) {
        this(String.format("Duplicate subscription for command [%s] detected. "
                                   + "Registration of handler [%s]  conflicts with previously registered handler [%s].",
                           name, initialHandler, duplicateHandler));
    }

    /**
     * Initializes a {@code DuplicateCommandHandlerSubscriptionException} using the given {@code message}.
     *
     * @param message The message describing the exception.
     */
    public DuplicateCommandHandlerSubscriptionException(String message) {
        super(message);
    }
}
