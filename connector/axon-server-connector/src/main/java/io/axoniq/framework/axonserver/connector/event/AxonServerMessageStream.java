/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.axonframework.common.StringUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;

import java.util.Optional;
import java.util.function.Function;

/**
 * A {@code MessageStream} implementation backed by a Stream of Events from Axon Server.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
class AxonServerMessageStream implements MessageStream<EventMessage> {

    private final EventStream stream;
    private final Function<Event, EventMessage> messageConverter;

    /**
     * Constructs the MessageStream, backed by given {@code stream} to AxonServer, using given {@code messageConverter}
     * to convert the Axon Server Events into Event Messages.
     *
     * @param stream           The backing stream to Axon Server to read from
     * @param messageConverter The function to convert Axon Server events to Event Messages
     */
    public AxonServerMessageStream(EventStream stream,
                                   Function<Event, EventMessage> messageConverter) {
        this.stream = stream;
        this.messageConverter = messageConverter;
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        EventWithToken eventWithToken = stream.nextIfAvailable();
        if (eventWithToken == null) {
            return Optional.empty();
        }
        SimpleEntry<EventMessage> entry = toSimpleEntry(eventWithToken);
        return Optional.of(entry);
    }

    private SimpleEntry<EventMessage> toSimpleEntry(EventWithToken eventWithToken) {
        Event event = eventWithToken.getEvent();
        EventMessage message = messageConverter.apply(event);
        GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(eventWithToken.getToken());
        Context context = Context.with(TrackingToken.RESOURCE_KEY, token);
        if (StringUtils.nonEmptyOrNull(event.getAggregateIdentifier())) {
            context = context.withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY,
                                           event.getAggregateSequenceNumber())
                             .withResource(LegacyResources.AGGREGATE_TYPE_KEY, event.getAggregateType())
                             .withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, event.getAggregateIdentifier());
        }
        return new SimpleEntry<>(message, context);
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        EventWithToken eventWithToken = stream.peek();
        if (eventWithToken == null) {
            return Optional.empty();
        }
        SimpleEntry<EventMessage> entry = toSimpleEntry(eventWithToken);
        return Optional.of(entry);
    }

    @Override
    public void setCallback(Runnable callback) {
        stream.onAvailable(callback);
    }

    @Override
    public Optional<Throwable> error() {
        return stream.getError();
    }

    @Override
    public boolean isCompleted() {
        return stream.isClosed();
    }

    @Override
    public boolean hasNextAvailable() {
        return stream.peek() != null;
    }

    @Override
    public void close() {
        stream.close();
    }
}
