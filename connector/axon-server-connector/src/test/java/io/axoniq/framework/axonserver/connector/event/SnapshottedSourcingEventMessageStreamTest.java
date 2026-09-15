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
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.SequencedEvent;
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
    void snapshotFollowedByAnEventIsPositionedFromTheFollowingEventsSequenceMinusOne() {
        // given
        Snapshot snapshot = snapshot();
        SequencedEvent event = sequencedEvent(5L);
        when(stream.nextIfAvailable()).thenReturn(
                snapshotResponse(snapshot), eventResponse(event), markerResponse(10L), null
        );
        when(stream.isClosed()).thenReturn(true);

        // when / then the snapshot entry is emitted first, positioned one before the following event's sequence
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
    void snapshotWithNoFollowingEventIsPositionedFromTheConsistencyMarker() {
        // given a snapshot immediately followed by the terminal marker - no events in between
        Snapshot snapshot = snapshot();
        when(stream.nextIfAvailable()).thenReturn(snapshotResponse(snapshot), markerResponse(7L), null);
        when(stream.isClosed()).thenReturn(true);

        // when / then the snapshot's position equals the marker, since no matching event with a greater
        // sequence exists
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
    void peekDoesNotConsumeTheResolvedEntry() {
        // given
        Snapshot snapshot = snapshot();
        when(stream.nextIfAvailable()).thenReturn(snapshotResponse(snapshot), markerResponse(3L), null);
        when(stream.isClosed()).thenReturn(true);

        // when peeking repeatedly
        MessageStream.Entry<EventMessage> firstPeek = testSubject.peek().orElseThrow();
        MessageStream.Entry<EventMessage> secondPeek = testSubject.peek().orElseThrow();

        // then it returns the same entry both times, and next() then returns that same entry too
        assertThat(firstPeek).isSameAs(secondPeek);
        assertThat(testSubject.next().orElseThrow()).isSameAs(firstPeek);
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

    private static SnapshottedSourceEventsResponse snapshotResponse(Snapshot snapshot) {
        return SnapshottedSourceEventsResponse.newBuilder().setSnapshot(snapshot).build();
    }

    private static SnapshottedSourceEventsResponse eventResponse(SequencedEvent event) {
        return SnapshottedSourceEventsResponse.newBuilder().setEvent(event).build();
    }

    private static SnapshottedSourceEventsResponse markerResponse(long marker) {
        return SnapshottedSourceEventsResponse.newBuilder().setConsistencyMarker(marker).build();
    }
}
