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

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Exception thrown whenever {@link QueryBus#subscriptionQuery(QueryMessage, ProcessingContext, int)} is
 * invoked multiple times for the same {@link QueryMessage}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class SubscriptionQueryAlreadyRegisteredException extends RuntimeException {

    /**
     * Constructs a {@code SubscriptionQueryAlreadyRegisteredException} for the given {@code queryId}.
     *
     * @param queryId The {@link QueryMessage#identifier()} of the subscription query accidentally being
     *                registered multiple times.
     */
    public SubscriptionQueryAlreadyRegisteredException(String queryId) {
        super("There is already a subscription query with query identifier [" + queryId + "] present.");
    }
}
