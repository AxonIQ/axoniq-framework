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

/**
 * Calculate the priority of {@link QueryMessage} based on its content.
 * <p>
 * Higher value means higher priority.
 *
 * @author Marc Gathier
 * @since 4.0.0
 */
@FunctionalInterface
public interface QueryPriorityCalculator {

    /**
     * Determines the priority of the given {@code query}. The higher the returned value, the higher the priority is.
     *
     * @param query a {@link QueryMessage} to prioritize
     * @return an {@code int} defining the priority of the given {@code query}
     */
    int determinePriority(QueryMessage query);

    /**
     * Returns a default implementation of the {@code QueryPriorityCalculator}, always returning priority {@code 0}.
     *
     * @return A lambda taking in a {@link QueryMessage} to prioritize to the default of priority {@code 0}.
     */
        static QueryPriorityCalculator defaultCalculator() {
        return query -> 0;
    }
}
