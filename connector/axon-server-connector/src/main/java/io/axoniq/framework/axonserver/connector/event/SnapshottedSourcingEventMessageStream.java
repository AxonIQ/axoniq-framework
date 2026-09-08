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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.SequencedEvent;
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
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link MessageStream} implementation backed by a {@link ResultStream} of
 * {@link SnapshottedSourceEventsResponse SnapshottedSourceEventsResponses} from Axon Server, translating the
 * {@code SnapshottedSourceEventsResponses} into {@link EventMessage EventMessages} as it moves along, prefixed by a
 * {@link SnapshotEventMessage} when Axon Server has a matching snapshot available.
 * <p>
 * This stream implementation buffers to ensure we can set the position on the snapshot we return. This in turn is
 * necessary as the {@code SnapshottedSourceEventsResponse} does not hold a sequence, only the events and the
 * consistency marker that follow it carry one. Hence, this stream implementation infers the snapshot's
 * {@link org.axonframework.eventsourcing.eventstore.Position Position} from the entry that comes after it. There are
 * two options for this, which are (1) the following event's sequence minus one, or (2) the terminal consistency marker
 * position.
 *
 * @author Steven van Beelen
 * @since 5.4.0
 */
class SnapshottedSourcingEventMessageStream implements MessageStream<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ResultStream<SnapshottedSourceEventsResponse> stream;
    private final TaggedEventConverter converter;
    private final Deque<Entry<EventMessage>> ready = new ArrayDeque<>();

    private io.axoniq.axonserver.grpc.event.dcb.@Nullable Snapshot pendingSnapshot;

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
        tryAdvance();
        return Optional.ofNullable(ready.poll());
    }

    @Override
    public Optional<Entry<EventMessage>> peek() {
        tryAdvance();
        return Optional.ofNullable(ready.peek());
    }

    /**
     * Pulls as much as is currently available from the underlying {@code stream} into {@link #ready}, resolving a
     * still-{@link #pendingSnapshot} against whichever item follows it. Does nothing once {@link #ready} already holds
     * an entry, so it never discards an entry that {@link #next()} has not yet consumed.
     */
    private void tryAdvance() {
        while (ready.isEmpty()) {
            SnapshottedSourceEventsResponse next = stream.nextIfAvailable();
            if (next == null) {
                logger.debug("Nothing available yet on the snapshotted source result stream.");
                return;
            } else if (next.hasSnapshot()) {
                logger.debug("Received a snapshot on the snapshotted source result stream; awaiting the item "
                                     + "that follows it to resolve its position.");
                pendingSnapshot = next.getSnapshot();
            } else if (next.hasEvent()) {
                SequencedEvent event = next.getEvent();
                resolvePendingSnapshot(event.getSequence() - 1);
                ready.add(convertToEventEntry(event));
            } else {
                long marker = next.getConsistencyMarker();
                logger.debug("Reached the consistency marker message of the snapshotted source result stream.");
                resolvePendingSnapshot(marker);
                ready.add(convertToMarkerEntry(marker));
            }
        }
    }

    private void resolvePendingSnapshot(long position) {
        if (pendingSnapshot != null) {
            ready.add(convertToSnapshotEntry(pendingSnapshot, position));
            pendingSnapshot = null;
        }
    }

    private Entry<EventMessage> convertToSnapshotEntry(io.axoniq.axonserver.grpc.event.dcb.Snapshot snapshot,
                                                       long position) {
        Snapshot domainSnapshot = new Snapshot(
                new GlobalIndexPosition(position),
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
        tryAdvance();
        return stream.isClosed() && ready.isEmpty();
    }

    @Override
    public boolean hasNextAvailable() {
        tryAdvance();
        return !ready.isEmpty();
    }

    @Override
    public void close() {
        stream.close();
    }
}
