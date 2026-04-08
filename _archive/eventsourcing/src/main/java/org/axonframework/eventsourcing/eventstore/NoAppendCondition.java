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

/**
 * An {@link AppendCondition} implementation that has {@link EventCriteria#havingAnyTag() no criteria}.
 * <p>
 * Only use this {@code AppendCondition} when appending events that <em>do not</em> partake in the consistency boundary
 * of any model(s).
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
final class NoAppendCondition implements AppendCondition {

    /**
     * Default instance of the {@link NoAppendCondition}.
     */
    static final NoAppendCondition INSTANCE = new NoAppendCondition();

    private NoAppendCondition() {
        // No-arg constructor to enforce use of INSTANCE constant.
    }

    @Override
    public ConsistencyMarker consistencyMarker() {
        return ConsistencyMarker.INFINITY;
    }

    @Override
    public EventCriteria criteria() {
        return EventCriteria.havingAnyTag();
    }

    @Override
    public AppendCondition withMarker(ConsistencyMarker consistencyMarker) {
        throw new UnsupportedOperationException("Cannot add a consistency marker without any criteria");
    }
}
