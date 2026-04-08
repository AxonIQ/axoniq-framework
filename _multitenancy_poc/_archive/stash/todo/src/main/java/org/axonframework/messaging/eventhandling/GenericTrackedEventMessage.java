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
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * Generic implementation of a {@link TrackedEventMessage}.
 *
 * @param <T> The type of payload contained in this Message
 * @deprecated In favor of pairing the {@link TrackingToken} through the
 * {@link MessageStream.Entry} its {@link Context} with an
 * {@link EventMessage}.
 */
@Deprecated
public class GenericTrackedEventMessage extends GenericEventMessage implements TrackedEventMessage {

    private final TrackingToken trackingToken;

    /**
     * Creates a GenericTrackedEventMessage with given {@code trackingToken} and delegate event message.
     *
     * @param trackingToken tracking token of the event
     * @param delegate      delegate message containing payload, metadata, identifier and timestamp
     */
    public GenericTrackedEventMessage(TrackingToken trackingToken, EventMessage delegate) {
        super(delegate, delegate.timestamp());
        this.trackingToken = trackingToken;
    }

    /**
     * Creates a GenericTrackedEventMessage with given {@code trackingToken} and delegate event message. The timestamp
     * of the event is supplied lazily to prevent unnecessary deserialization of the timestamp.
     *
     * @param trackingToken tracking token of the event
     * @param delegate      delegate message containing payload, metadata, identifier and timestamp
     * @param timestamp     supplier of the message's timestamp
     */
    public GenericTrackedEventMessage(TrackingToken trackingToken, Message delegate, Supplier<Instant> timestamp) {
        super(delegate, timestamp);
        this.trackingToken = trackingToken;
    }

    /**
     * Initializes a {@link GenericTrackedEventMessage} with given message as delegate and given {@code timestamp}. The
     * given message will be used supply the payload, metadata and identifier of the resulting event message.
     *
     * @param trackingToken the tracking token of the resulting message
     * @param delegate      the message that will be used as delegate
     * @param timestamp     the timestamp of the resulting event message
     */
    protected GenericTrackedEventMessage(TrackingToken trackingToken, Message delegate, Instant timestamp) {
        super(delegate, timestamp);
        this.trackingToken = trackingToken;
    }

    @Override
    public TrackingToken trackingToken() {
        return trackingToken;
    }

    @Override
    protected void describeTo(StringBuilder stringBuilder) {
        super.describeTo(stringBuilder);
        stringBuilder.append(", trackingToken={")
                     .append(trackingToken())
                     .append('}');
    }

    @Override
    public GenericTrackedEventMessage withTrackingToken(TrackingToken trackingToken) {
        return new GenericTrackedEventMessage(trackingToken, this);
    }

    @Override
    protected String describeType() {
        return "GenericTrackedEventMessage";
    }
}
