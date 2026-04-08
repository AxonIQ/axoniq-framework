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

package org.axonframework.messaging.eventhandling;

import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.conversion.SerializedObject;

import java.time.Instant;

/**
 * Specialization of the DomainEventData class that includes the Token representing the position of this event in
 * a stream.
 *
 * @param <T> The content type of the serialized data
 * @deprecated Will be removed entirely in favor of the {@link EventMessage}.
 */
@Deprecated(since = "5.0.0", forRemoval = true)
public class TrackedDomainEventData<T> implements TrackedEventData<T>, DomainEventData<T> {

    private final TrackingToken trackingToken;
    private final DomainEventData<T> eventData;

    /**
     * Initialize the TrackingDomainEventData with given {@code trackingToken} and {@code domainEventEntry}.
     *
     * @param trackingToken    The token representing this event's position in a stream
     * @param domainEventEntry The entry containing the event data itself
     */
    public TrackedDomainEventData(TrackingToken trackingToken, DomainEventData<T> domainEventEntry) {
        this.trackingToken = trackingToken;
        this.eventData = domainEventEntry;
    }

    @Override
    public TrackingToken trackingToken() {
        return trackingToken;
    }

    @Override
    public String getEventIdentifier() {
        return eventData.getEventIdentifier();
    }

    @Override
    public Instant getTimestamp() {
        return eventData.getTimestamp();
    }

    @Override
    public SerializedObject<T> getMetadata() {
        return eventData.getMetadata();
    }

    @Override
    public SerializedObject<T> getPayload() {
        return eventData.getPayload();
    }

    @Override
    public String getType() {
        return eventData.getType();
    }

    @Override
    public String getAggregateIdentifier() {
        return eventData.getAggregateIdentifier();
    }

    @Override
    public long getSequenceNumber() {
        return eventData.getSequenceNumber();
    }
}
