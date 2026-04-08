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
 * Default implementation of the {@link AppendCondition}, using the given {@code consistencyMarker} and {@code criteria}
 * as output for the {@link #consistencyMarker()} and {@link #criteria()} operations respectively.
 *
 * @param consistencyMarker The consistency marker obtained while sourcing events.
 * @param criteria          The criteria set defining which changes are considered conflicting.
 * @author Steven van Beelen
 * @since 5.0.0
 */
record DefaultAppendCondition(
        ConsistencyMarker consistencyMarker,
        EventCriteria criteria
) implements AppendCondition {

    DefaultAppendCondition {
        requireNonNull(consistencyMarker, "The ConsistencyMarker cannot be null");
        requireNonNull(criteria, "The EventCriteria cannot be null");
    }

    @Override
    public AppendCondition withMarker(ConsistencyMarker consistencyMarker) {
        if (this.consistencyMarker.equals(consistencyMarker)) {
            return this;
        }
        return new DefaultAppendCondition(consistencyMarker, criteria);
    }
}
