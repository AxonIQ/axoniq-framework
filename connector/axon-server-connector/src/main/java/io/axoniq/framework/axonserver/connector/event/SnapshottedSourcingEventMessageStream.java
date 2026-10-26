/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.SequencedEvent;
import io.axoniq.axonserver.grpc.event.dcb.SequencedSnapshot;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceEventsResponse;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.SimpleEntry;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} implementation backed by a {@link ResultStream} of
 * {@link SnapshottedSourceEventsResponse SnapshottedSourceEventsResponses} from Axon Server, translating the
 * {@code SnapshottedSourceEventsResponses} into {@link EventMessage EventMessages} as it moves along, prefixed by a
 * {@link SnapshotEventMessage} when Axon Server has a matching snapshot available.
 * <p>
 * Each response is translated directly, with no buffering - the {@code SequencedSnapshot} a node advertising
 * {@code dcb:snapshot-aware-sourcing} returns already carries the snapshot's own sequence, so this stream no longer
 * needs to infer it from whichever entry happens to follow it.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
class SnapshottedSourcingEventMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ResultStream<SnapshottedSourceEventsResponse> stream;
    private final TaggedEventConverter converter;

    /**
     * Constructs a {@code SnapshottedSourcingEventMessageStream} with the given {@code stream} and {@code converter}.
     *
     * @param stream    The {@code ResultStream} of {@code SnapshottedSourceEventsResponses} to convert into
     *                  {@link EventMessage EventMessages} for this {@link MessageStream} implementation.
     * @param converter The {@code TaggedEventConverter} used to convert events into {@link EventMessage EventMessages}
     *                  for this {@link MessageStream} implementation.
     */
    public SnapshottedSourcingEventMessageStream(ResultStream<SnapshottedSourceEventsResponse> stream,
                                                 TaggedEventConverter converter) {
        this.stream = Objects.requireNonNull(stream, "The source result stream cannot be null.");
        this.converter = Objects.requireNonNull(converter, "The converter cannot be null.");
    }

    @Override
    public Optional<Entry<EventMessage>> next() {
        SnapshottedSourceEventsResponse next = stream.nextIfAvailable();
        if (next == null) {
            logger.debug("Nothing available yet on the snapshotted source result stream.");
            return Optional.empty();
        }
        return Optional.of(convert(next));
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        SnapshottedSourceEventsResponse peeked = stream.peek();
        return peeked == null ? Optional.empty() : Optional.of(convert(peeked));
    }

    private Entry<EventMessage> convert(SnapshottedSourceEventsResponse response) {
        if (response.hasSequencedSnapshot()) {
            return convertToSnapshotEntry(response.getSequencedSnapshot());
        } else if (response.hasEvent()) {
            return convertToEventEntry(response.getEvent());
        }
        return convertToMarkerEntry(response.getConsistencyMarker());
    }

    private Entry<EventMessage> convertToSnapshotEntry(SequencedSnapshot sequencedSnapshot) {
        io.axoniq.axonserver.grpc.event.dcb.Snapshot snapshot = sequencedSnapshot.getSnapshot();
        Snapshot domainSnapshot = new Snapshot(
                new GlobalIndexPosition(sequencedSnapshot.getSequence()),
                snapshot.getVersion(),
                snapshot.getPayload().toByteArray(),
                Instant.ofEpochMilli(snapshot.getTimestamp()),
                snapshot.getMetadataMap()
        );
        return new SimpleEntry<>(new SnapshotEventMessage(domainSnapshot), Context.empty());
    }

    private Entry<EventMessage> convertToEventEntry(SequencedEvent event) {
        EventMessage eventMessage = converter.convertEvent(event.getEvent());
        TrackingToken token = new GlobalSequenceTrackingToken(event.getSequence() + 1);
        Context context = Context.with(TrackingToken.RESOURCE_KEY, token);
        return new SimpleEntry<>(eventMessage, context);
    }

    private static Entry<EventMessage> convertToMarkerEntry(long marker) {
        Context context = ConsistencyMarker.addToContext(
                Context.empty(), new GlobalIndexConsistencyMarker(marker)
        );
        return new SimpleEntry<>(TerminalEventMessage.INSTANCE, context);
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
        if (!stream.isClosed()) {
            stream.close();
        }
    }
}
