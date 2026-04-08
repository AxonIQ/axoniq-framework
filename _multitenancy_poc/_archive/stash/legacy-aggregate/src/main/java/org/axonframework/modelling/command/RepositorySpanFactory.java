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

package org.axonframework.modelling.command;

import org.axonframework.messaging.tracing.Span;

/**
 * Span factory that creates spans for the {@link Repository}. You can customize the spans of the bus by creating
 * your own implementation.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface RepositorySpanFactory {

    /**
     * Creates a span that represents the loading of an aggregate with the provided identifier.
     *
     * @param aggregateId The identifier of the aggregate that is being loaded.
     * @return A span that represents the loading of the aggregate.
     */
    Span createLoadSpan(String aggregateId);

    /**
     * Creates a span that represents the time waiting to acquire a lock on an aggregate with the provided identifier.
     *
     * @param aggregateId The identifier of the aggregate that is trying to acquire a lock.
     * @return A span that represents the acquisition of the lock for the aggregate.
     */
    Span createObtainLockSpan(String aggregateId);

    /**
     * Creates a span that represents the time it took to hydrate the aggregate with data from, for example, the event
     * store.
     *
     * @param aggregateType The type of the aggregate that is being hydrated.
     * @param aggregateId   The identifier of the aggregate that is being hydrated.
     * @return A span that represents the hydration of the aggregate.
     */
    Span createInitializeStateSpan(String aggregateType, String aggregateId);
}
