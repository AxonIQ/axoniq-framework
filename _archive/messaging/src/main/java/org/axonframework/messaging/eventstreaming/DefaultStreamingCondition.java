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

package org.axonframework.messaging.eventstreaming;

import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of the {@link StreamingCondition}.
 *
 * @param position The {@link TrackingToken} defining the {@link #position()} of this condition.
 * @param criteria The {@link EventCriteria} defining the {@link #criteria()} of this condition.
 * @author Steven van Beelen
 * @since 5.0.0
 */
record DefaultStreamingCondition(
        TrackingToken position,
        EventCriteria criteria
) implements StreamingCondition {

    DefaultStreamingCondition {
        requireNonNull(position, "The position cannot be null");
        requireNonNull(criteria, "The EventCriteria cannot be null");
    }

    @Override
    public StreamingCondition or(EventCriteria criteria) {
        return new DefaultStreamingCondition(this.position, this.criteria.or(criteria));
    }
}
