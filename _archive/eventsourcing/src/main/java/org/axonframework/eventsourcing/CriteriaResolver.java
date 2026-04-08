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

package org.axonframework.eventsourcing;

import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Functional interface describing a resolver of an {@link EventCriteria} based on an identifier of type {@code I}.
 *
 * @param <I> The type of identifier to resolve to an {@link EventCriteria}.
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface CriteriaResolver<I> {

    /**
     * Resolves the given {@code identifier} to an {@link EventCriteria}.
     *
     * @param identifier The instance to resolve to an {@link EventCriteria}.
     * @param context    The {@link ProcessingContext} in which the criteria is being resolved.
     * @return The given {@code identifier} resolved to an {@link EventCriteria}.
     */
    EventCriteria resolve(I identifier, ProcessingContext context);
}
