/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.SequencedEvent;
import io.axoniq.axonserver.grpc.event.dcb.SequencedSnapshot;
import io.axoniq.axonserver.grpc.event.dcb.Snapshot;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceEventsResponse;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link SnapshottedSourcingEventMessageStream}.
 *
 * @author Steven van Beelen
 */
class SnapshottedSourcingEventMessageStreamTest {

    private static final ByteString SNAPSHOT_KEY = ByteString.copyFromUtf8("snapshot-key");

    private ResultStream<SnapshottedSourceEventsResponse> stream;
    private SnapshottedSourcingEventMessageStream testSubject;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        stream = mock(ResultStream.class);
        when(stream.getError()).thenReturn(Optional.empty());

        TaggedEventConverter converter =
                new TaggedEventConverter(new DelegatingEventConverter(new JacksonConverter()), EventTypeResolver.DEFAULT);
        testSubject = new SnapshottedSourcingEventMessageStream(stream, converter);
    }

    @Test
    void snapshotIsPositionedFromItsOwnSequenceRegardlessOfWhatFollowsIt() {
        // given a snapshot taken at sequence 4, followed by an event and the terminal marker - neither of which
        // the snapshot's position should depend on
        Snapshot snapshot = snapshot();
        SequencedEvent event = sequencedEvent(5L);
        when(stream.nextIfAvailable()).thenReturn(
                sequencedSnapshotResponse(4L, snapshot), eventResponse(event), markerResponse(10L), null
        );
        when(stream.isClosed()).thenReturn(true);

        // when / then the snapshot entry is emitted first, positioned at its own sequence
        MessageStream.Entry<EventMessage> snapshotEntry = testSubject.next().orElseThrow();
        assertThat(snapshotEntry.message()).isInstanceOf(SnapshotEventMessage.class);
        org.axonframework.eventsourcing.snapshot.api.Snapshot payload =
                (org.axonframework.eventsourcing.snapshot.api.Snapshot) snapshotEntry.message().payload();
        assertThat(payload.position()).isEqualTo(new GlobalIndexPosition(4L));
        assertThat(payload.version()).isEqualTo(snapshot.getVersion());

        // and then the event that followed it
        MessageStream.Entry<EventMessage> eventEntry = testSubject.next().orElseThrow();
        assertThat(eventEntry.message()).isNotInstanceOf(SnapshotEventMessage.class);
        assertThat(eventEntry.message()).isNotInstanceOf(TerminalEventMessage.class);

        // and finally the terminal marker
        MessageStream.Entry<EventMessage> markerEntry = testSubject.next().orElseThrow();
        assertThat(markerEntry.message()).isInstanceOf(TerminalEventMessage.class);
    }

    @Test
    void snapshotWithNoFollowingEventStillUsesItsOwnSequence() {
        // given a snapshot taken at sequence 7, immediately followed by an unrelated terminal marker - no events
        // in between, and no event following it at all
        Snapshot snapshot = snapshot();
        when(stream.nextIfAvailable()).thenReturn(sequencedSnapshotResponse(7L, snapshot), markerResponse(99L), null);
        when(stream.isClosed()).thenReturn(true);

        // when / then the snapshot's position is its own sequence, not derived from the unrelated marker
        MessageStream.Entry<EventMessage> snapshotEntry = testSubject.next().orElseThrow();
        org.axonframework.eventsourcing.snapshot.api.Snapshot payload =
                (org.axonframework.eventsourcing.snapshot.api.Snapshot) snapshotEntry.message().payload();
        assertThat(payload.position()).isEqualTo(new GlobalIndexPosition(7L));

        MessageStream.Entry<EventMessage> markerEntry = testSubject.next().orElseThrow();
        assertThat(markerEntry.message()).isInstanceOf(TerminalEventMessage.class);
    }

    @Test
    void noSnapshotBehavesLikeAPlainEventStream() {
        // given no snapshot is present at all
        SequencedEvent event = sequencedEvent(0L);
        when(stream.nextIfAvailable()).thenReturn(eventResponse(event), markerResponse(0L), null);
        when(stream.isClosed()).thenReturn(true);

        // when / then
        MessageStream.Entry<EventMessage> eventEntry = testSubject.next().orElseThrow();
        assertThat(eventEntry.message()).isNotInstanceOf(SnapshotEventMessage.class);

        MessageStream.Entry<EventMessage> markerEntry = testSubject.next().orElseThrow();
        assertThat(markerEntry.message()).isInstanceOf(TerminalEventMessage.class);
    }

    @Test
    void peekDoesNotConsumeTheUnderlyingStream() {
        // given
        when(stream.peek()).thenReturn(markerResponse(3L));

        // when peeking repeatedly
        MessageStream.Entry<EventMessage> firstPeek = testSubject.peek().orElseThrow();
        MessageStream.Entry<EventMessage> secondPeek = testSubject.peek().orElseThrow();

        // then the underlying stream is only ever peeked, never advanced
        assertThat(firstPeek.message()).isInstanceOf(TerminalEventMessage.class);
        assertThat(secondPeek.message()).isInstanceOf(TerminalEventMessage.class);
        verify(stream, never()).nextIfAvailable();
    }

    @Nested
    class Close {

        @Test
        void doesNotCancelAStreamTheServerAlreadyCompleted() {
            // given the server completed the stream and all responses were consumed
            when(stream.isClosed()).thenReturn(true);

            // when
            testSubject.close();

            // then cancelling is skipped, as it would record a client-side close error on a completed stream
            verify(stream, never()).close();
        }

        @Test
        void cancelsAStreamThatIsStillOpen() {
            // given
            when(stream.isClosed()).thenReturn(false);

            // when
            testSubject.close();

            // then
            verify(stream).close();
        }
    }

    private static Snapshot snapshot() {
        return Snapshot.newBuilder()
                       .setName("snapshot-name")
                       .setVersion("0.0.1")
                       .setTimestamp(Instant.now().toEpochMilli())
                       .build();
    }

    private static SequencedEvent sequencedEvent(long sequence) {
        Event event = Event.newBuilder()
                           .setIdentifier(UUID.randomUUID().toString())
                           .setTimestamp(Instant.now().toEpochMilli())
                           .setName("test-event")
                           .setVersion("0.0.1")
                           .build();
        return SequencedEvent.newBuilder().setEvent(event).setSequence(sequence).build();
    }

    private static SnapshottedSourceEventsResponse sequencedSnapshotResponse(long sequence, Snapshot snapshot) {
        return SnapshottedSourceEventsResponse.newBuilder()
                                              .setSequencedSnapshot(SequencedSnapshot.newBuilder()
                                                                                      .setKey(SNAPSHOT_KEY)
                                                                                      .setSequence(sequence)
                                                                                      .setSnapshot(snapshot))
                                              .build();
    }

    private static SnapshottedSourceEventsResponse eventResponse(SequencedEvent event) {
        return SnapshottedSourceEventsResponse.newBuilder().setEvent(event).build();
    }

    private static SnapshottedSourceEventsResponse markerResponse(long marker) {
        return SnapshottedSourceEventsResponse.newBuilder().setConsistencyMarker(marker).build();
    }
}
