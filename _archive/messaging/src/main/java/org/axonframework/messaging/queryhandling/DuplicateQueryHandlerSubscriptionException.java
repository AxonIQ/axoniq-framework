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

package org.axonframework.messaging.queryhandling;

import org.axonframework.messaging.core.QualifiedName;

/**
 * Exception indicating a duplicate {@link QueryHandler} was subscribed.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class DuplicateQueryHandlerSubscriptionException extends RuntimeException {

    /**
     * Initialize a duplicate {@link QueryHandler} subscription exception using the given {@code initialHandler} and
     * {@code duplicateHandler} to form a specific message.
     *
     * @param name             The name of the query handler for which the duplicate was detected.
     * @param initialHandler   the initial {@link QueryHandler} for which a duplicate was encountered.
     * @param duplicateHandler The duplicated {@link QueryHandler}.
     */
    public DuplicateQueryHandlerSubscriptionException(QualifiedName name,
                                                      QueryHandler initialHandler,
                                                      QueryHandler duplicateHandler) {
        this(String.format("Duplicate subscription for query handler [%s] detected. "
                                   + "Registration of handler [%s]  conflicts with previously registered handler [%s].",
                           name, initialHandler, duplicateHandler));
    }

    /**
     * Initializes a {@code DuplicateQueryHandlerSubscriptionException} using the given {@code message}.
     *
     * @param message The message describing the exception.
     */
    public DuplicateQueryHandlerSubscriptionException(String message) {
        super(message);
    }
}
