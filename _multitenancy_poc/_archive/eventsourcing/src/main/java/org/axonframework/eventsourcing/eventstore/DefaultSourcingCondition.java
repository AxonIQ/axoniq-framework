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

package org.axonframework.eventsourcing.eventstore;

import org.axonframework.messaging.eventstreaming.EventCriteria;

import static java.util.Objects.requireNonNull;

/**
 * The default {@link SourcingCondition} implementation.
 * <p>
 * The {@code start} refers to the start point of the event stream that is of interest to this
 * {@link SourcingCondition}.
 *
 * @param start    The start position in the event sequence to retrieve of the entity to source.
 * @param criteria The {@link EventCriteria} set of the entity to source.
 * @author Steven van Beelen
 * @author John Hendrikx
 * @since 5.0.0
 */
record DefaultSourcingCondition(
        Position start,
        EventCriteria criteria
) implements SourcingCondition {

    DefaultSourcingCondition {
        requireNonNull(start, "start cannot be null");
        requireNonNull(criteria, "criteria cannot be null");
    }

    @Override
    public SourcingCondition or(SourcingCondition other) {
        return new DefaultSourcingCondition(
            other.start().min(start),
            other.criteria().or(criteria)
        );
    }
}
