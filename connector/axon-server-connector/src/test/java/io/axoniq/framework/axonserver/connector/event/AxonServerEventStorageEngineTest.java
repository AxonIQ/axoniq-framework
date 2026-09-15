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

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.SequencedEvent;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceEventsResponse;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsResponse;
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.grpc.Status;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.AppendEventsTransactionRejectedException;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.*;
import org.mockito.*;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AxonServerEventStorageEngine} through mocking.
 * <p>
 * For an integration tests, use the {@link AxonServerEventStorageEngineIT} instead.
 *
 * @author Steven van Beelen
 */
class AxonServerEventStorageEngineTest {

    private static final String EVENT_NAME = "test-event";

    private AxonServerConnection connection;
    private DcbEventChannel dcbEventChannel;
    private ResultStream<SourceEventsResponse> sourcingStream;
    private SnapshotChannel snapshotChannel;
    private EventConverter eventConverter;

    private AxonServerEventStorageEngine testSubject;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        connection = mock(AxonServerConnection.class);
        dcbEventChannel = mock(DcbEventChannel.class);
        sourcingStream = mock(ResultStream.class);
        snapshotChannel = mock(SnapshotChannel.class);
        eventConverter = new DelegatingEventConverter(new JacksonConverter());

        when(connection.dcbEventChannel()).thenReturn(dcbEventChannel);
        when(connection.snapshotChannel()).thenReturn(snapshotChannel);
        when(dcbEventChannel.source(any(SourceEventsRequest.class))).thenReturn(sourcingStream);
        when(sourcingStream.getError()).thenReturn(java.util.Optional.empty());
        when(sourcingStream.isClosed()).thenReturn(true);

        testSubject = new AxonServerEventStorageEngine(connection, eventConverter);
    }

    @Test
    void defaultResolverSubstitutesEmptyVersionWithMissingVersionDefault() {
        // given
        Event storedEvent = Event.newBuilder()
                                 .setIdentifier(UUID.randomUUID().toString())
                                 .setTimestamp(Instant.now().toEpochMilli())
                                 .setName(EVENT_NAME)
                                 // no setVersion() -- proto default is empty string
                                 .build();
        SourceEventsResponse eventResponse = SourceEventsResponse.newBuilder()
                                                                 .setEvent(SequencedEvent.newBuilder()
                                                                                         .setEvent(storedEvent)
                                                                                         .setSequence(0L)
                                                                                         .build())
                                                                 .build();
        SourceEventsResponse markerResponse = SourceEventsResponse.newBuilder()
                                                                  .setConsistencyMarker(0L)
                                                                  .build();
        when(sourcingStream.nextIfAvailable()).thenReturn(eventResponse, markerResponse, null);
        when(sourcingStream.peek()).thenReturn(eventResponse, markerResponse, null);
        SourcingCondition condition = SourcingCondition.conditionFor(
                EventCriteria.havingTags("AGGREGATE_TYPE", UUID.randomUUID().toString())
        );
        // when / then
        StepVerifier.create(FluxUtils.of(testSubject.source(condition)))
                    .assertNext(entry -> {
                        assertThat(entry.message()).isNotInstanceOf(TerminalEventMessage.class);
                        assertThat(entry.message().type().name()).isEqualTo(EVENT_NAME);
                        assertThat(entry.message().type().version())
                                .isEqualTo(EventTypeResolver.MISSING_VERSION_DEFAULT);
                    })
                    .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                    .verifyComplete();
    }

    @Nested
    class SnapshotSupport {

        private final QualifiedName qualifiedName = new QualifiedName("test-entity");
        private final String identifier = "entity-id";

        private AxonServerSnapshotStore snapshotStore;

        @BeforeEach
        void setUp() {
            snapshotStore = new AxonServerSnapshotStore(connection, eventConverter);
        }

        @Test
        void storeDelegatesToTheInternalSnapshotStoreUsingTheSharedKeyFormat() {
            // given
            when(snapshotChannel.addSnapshot(any()))
                    .thenReturn(CompletableFuture.completedFuture(AddSnapshotResponse.newBuilder().build()));
            Snapshot snapshot = new Snapshot(
                    new GlobalIndexPosition(4L), "0.0.1", "payload", Instant.now(), Map.of()
            );

            // when
            testSubject.store(qualifiedName, identifier, snapshot, null).orTimeout(5, TimeUnit.SECONDS).join();

            // then the key sent to Axon Server matches the single owner of the snapshot key wire format
            ArgumentCaptor<AddSnapshotRequest> captor = ArgumentCaptor.forClass(AddSnapshotRequest.class);
            verify(snapshotChannel).addSnapshot(captor.capture());
            ByteString expectedKey = snapshotStore.snapshotKey(qualifiedName, identifier);
            assertThat(captor.getValue().getKey()).isEqualTo(expectedKey);
        }

        @Test
        void loadDelegatesToTheInternalSnapshotStoreUsingTheSharedKeyFormat() {
            // given
            io.axoniq.axonserver.grpc.event.dcb.Snapshot storedSnapshot =
                    io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                                .setName(qualifiedName.fullName())
                                                                .setVersion("0.0.1")
                                                                .setTimestamp(Instant.now().toEpochMilli())
                                                                .putMetadata("__AxonFramework__:Position-Type", "GIP")
                                                                .build();
            when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    GetLastSnapshotResponse.newBuilder().setSnapshot(storedSnapshot).setSequence(42L).build()
            ));

            // when
            Snapshot result = testSubject.load(qualifiedName, identifier, null)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();

            // then
            assertThat(result).isNotNull();
            assertThat(result.position()).isEqualTo(new GlobalIndexPosition(42L));
            ArgumentCaptor<GetLastSnapshotRequest> captor = ArgumentCaptor.forClass(GetLastSnapshotRequest.class);
            verify(snapshotChannel).getLastSnapshot(captor.capture());
            ByteString expectedKey = snapshotStore.snapshotKey(qualifiedName, identifier);
            assertThat(captor.getValue().getKey()).isEqualTo(expectedKey);
        }

        @Test
        @SuppressWarnings("unchecked")
        void sourceWithUnboundedSnapshotStrategyEmitsStoredSnapshotBeforeSubsequentEvents() {
            // given a snapshot followed by one event and the consistency marker on the single-round-trip stream
            ResultStream<SnapshottedSourceEventsResponse> snapshottedStream = mock(ResultStream.class);
            when(snapshottedStream.getError()).thenReturn(java.util.Optional.empty());
            when(snapshottedStream.isClosed()).thenReturn(true);

            io.axoniq.axonserver.grpc.event.dcb.Snapshot storedSnapshot =
                    io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                                .setName(qualifiedName.fullName())
                                                                .setVersion("0.0.1")
                                                                .setTimestamp(Instant.now().toEpochMilli())
                                                                .build();
            Event storedEvent = Event.newBuilder()
                                     .setIdentifier(UUID.randomUUID().toString())
                                     .setTimestamp(Instant.now().toEpochMilli())
                                     .setName(EVENT_NAME)
                                     .setVersion("0.0.1")
                                     .build();
            when(snapshottedStream.nextIfAvailable()).thenReturn(
                    SnapshottedSourceEventsResponse.newBuilder().setSnapshot(storedSnapshot).build(),
                    SnapshottedSourceEventsResponse.newBuilder()
                                                   .setEvent(SequencedEvent.newBuilder()
                                                                           .setEvent(storedEvent)
                                                                           .setSequence(5L)
                                                                           .build())
                                                   .build(),
                    SnapshottedSourceEventsResponse.newBuilder().setConsistencyMarker(5L).build(),
                    null
            );
            when(dcbEventChannel.source(any(SnapshottedSourceRequest.class))).thenReturn(snapshottedStream);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then the snapshot comes first, positioned right before the event that follows it
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .assertNext(entry -> {
                            assertThat(entry.message()).isInstanceOf(SnapshotEventMessage.class);
                            Snapshot emitted = ((SnapshotEventMessage) entry.message()).payload();
                            assertThat(emitted.position()).isEqualTo(new GlobalIndexPosition(4L));
                        })
                        .assertNext(entry -> assertThat(entry.message().type().name()).isEqualTo(EVENT_NAME))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            verify(dcbEventChannel).source(any(SnapshottedSourceRequest.class));
            verify(dcbEventChannel, never()).source(any(SourceEventsRequest.class));
        }

        @Test
        @SuppressWarnings("unchecked")
        void sourceWithUnboundedSnapshotStrategyFallsBackToLoadingSeparatelyWhenAxonServerDoesNotSupportSnapshotting() {
            // given the single-round-trip snapshotted-source RPC failing with UNIMPLEMENTED, as an older Axon
            // Server would report, but the separate snapshot-store RPC still finding a snapshot
            ResultStream<SnapshottedSourceEventsResponse> snapshottedStream = mock(ResultStream.class);
            when(snapshottedStream.getError())
                    .thenReturn(java.util.Optional.of(Status.UNIMPLEMENTED.asRuntimeException()));
            when(dcbEventChannel.source(any(SnapshottedSourceRequest.class))).thenReturn(snapshottedStream);

            io.axoniq.axonserver.grpc.event.dcb.Snapshot storedSnapshot =
                    io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                                .setName(qualifiedName.fullName())
                                                                .setVersion("0.0.1")
                                                                .setTimestamp(Instant.now().toEpochMilli())
                                                                .putMetadata("__AxonFramework__:Position-Type", "GIP")
                                                                .build();
            when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    GetLastSnapshotResponse.newBuilder().setSnapshot(storedSnapshot).setSequence(50L).build()
            ));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(50L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(50L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then the snapshot is still prepended, fetched through the separate load() RPC, followed by
            // events sourced from its position -- snapshots are not disabled just because the single-round-trip
            // RPC is unsupported
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .assertNext(entry -> assertThat(entry.message()).isInstanceOf(SnapshotEventMessage.class))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            verify(snapshottedStream).close();
            verify(snapshotChannel).getLastSnapshot(any());
            ArgumentCaptor<SourceEventsRequest> captor = ArgumentCaptor.forClass(SourceEventsRequest.class);
            verify(dcbEventChannel).source(captor.capture());
            assertThat(captor.getValue().getFromSequence()).isEqualTo(50L);
        }

        @Test
        @SuppressWarnings("unchecked")
        void sourceWithUnboundedSnapshotStrategyOnlyFallsBackOnceWhenTheAvailabilityCallbackRacesTheInlineCheck()
                throws InterruptedException {
            // given the onAvailable callback firing on its own thread, concurrently with the inline availability
            // check the engine performs right after registering it -- both invocations see the same UNIMPLEMENTED
            // error and could otherwise both trigger the fallback
            ResultStream<SnapshottedSourceEventsResponse> snapshottedStream = mock(ResultStream.class);
            when(snapshottedStream.getError())
                    .thenReturn(java.util.Optional.of(Status.UNIMPLEMENTED.asRuntimeException()));
            // The inline checkAvailability.run() the engine performs right after registering the callback already
            // runs synchronously on this thread, so only the callback thread below needs to be awaited.
            CountDownLatch callbackThreadDone = new CountDownLatch(1);
            doAnswer(invocation -> {
                Runnable callback = invocation.getArgument(0);
                new Thread(() -> {
                    callback.run();
                    callbackThreadDone.countDown();
                }).start();
                return null;
            }).when(snapshottedStream).onAvailable(any());
            when(dcbEventChannel.source(any(SnapshottedSourceRequest.class))).thenReturn(snapshottedStream);

            when(snapshotChannel.getLastSnapshot(any()))
                    .thenReturn(CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException()));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when
            MessageStream<EventMessage> stream = testSubject.source(condition, null);
            callbackThreadDone.await(2, TimeUnit.SECONDS);
            StepVerifier.create(FluxUtils.of(stream))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            // then despite both the inline check and the callback thread having run, the fallback only fired once
            verify(snapshottedStream, times(1)).close();
            verify(snapshotChannel, times(1)).getLastSnapshot(any());
        }

        @Test
        @SuppressWarnings("unchecked")
        void sourceWithUnboundedSnapshotStrategySkipsTheSingleRoundTripRpcAfterItWasFoundUnimplementedOnce() {
            // given the snapshotted-source RPC failing with UNIMPLEMENTED
            ResultStream<SnapshottedSourceEventsResponse> snapshottedStream = mock(ResultStream.class);
            when(snapshottedStream.getError())
                    .thenReturn(java.util.Optional.of(Status.UNIMPLEMENTED.asRuntimeException()));
            when(dcbEventChannel.source(any(SnapshottedSourceRequest.class))).thenReturn(snapshottedStream);

            when(snapshotChannel.getLastSnapshot(any()))
                    .thenReturn(CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException()));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when sourcing once discovers the RPC is unsupported and falls back
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();
            verify(dcbEventChannel, times(1)).source(any(SnapshottedSourceRequest.class));

            // when sourcing again
            testSubject.source(condition, null);

            // then the single-round-trip RPC is not attempted a second time -- the engine remembered it's unsupported
            verify(dcbEventChannel, times(1)).source(any(SnapshottedSourceRequest.class));
            verify(dcbEventChannel, times(2)).source(any(SourceEventsRequest.class));
        }

        @Test
        @SuppressWarnings("unchecked")
        void sourceWithUnboundedSnapshotStrategyFallsBackToFullReconstructionWhenNeitherAxonServerNorASnapshotAreAvailable() {
            // given the single-round-trip snapshotted-source RPC failing with UNIMPLEMENTED, and the separate
            // snapshot-store RPC finding no snapshot either
            ResultStream<SnapshottedSourceEventsResponse> snapshottedStream = mock(ResultStream.class);
            when(snapshottedStream.getError())
                    .thenReturn(java.util.Optional.of(Status.UNIMPLEMENTED.asRuntimeException()));
            when(dcbEventChannel.source(any(SnapshottedSourceRequest.class))).thenReturn(snapshottedStream);

            when(snapshotChannel.getLastSnapshot(any()))
                    .thenReturn(CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException()));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, null),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then only now, with no snapshot available through either RPC, falls back to full reconstruction
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            verify(snapshottedStream).close();
            ArgumentCaptor<SourceEventsRequest> captor = ArgumentCaptor.forClass(SourceEventsRequest.class);
            verify(dcbEventChannel).source(captor.capture());
            assertThat(captor.getValue().getFromSequence()).isEqualTo(GlobalIndexPosition.toIndex(Position.START));
        }

        @Test
        void sourceWithBoundedSnapshotStrategyFallsBackToLoadingSeparately() {
            // given no snapshot exists, simulated the way AxonServerSnapshotStore.load() recognises "not found"
            when(snapshotChannel.getLastSnapshot(any()))
                    .thenReturn(CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException()));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, new GlobalIndexPosition(100L)),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then falls back to sourcing from the very beginning, through the plain (non-snapshotted) RPC
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            verify(snapshotChannel).getLastSnapshot(any());
            ArgumentCaptor<SourceEventsRequest> captor = ArgumentCaptor.forClass(SourceEventsRequest.class);
            verify(dcbEventChannel).source(captor.capture());
            assertThat(captor.getValue().getFromSequence()).isEqualTo(GlobalIndexPosition.toIndex(Position.START));
            verify(dcbEventChannel, never()).source(any(SnapshottedSourceRequest.class));
        }

        @Test
        void sourceWithBoundedSnapshotStrategyPrependsSnapshotWhenWithinMaximumPosition() {
            // given a snapshot at position 50, within the requested maximum position of 100
            io.axoniq.axonserver.grpc.event.dcb.Snapshot storedSnapshot =
                    io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                                .setName(qualifiedName.fullName())
                                                                .setVersion("0.0.1")
                                                                .putMetadata("__AxonFramework__:Position-Type", "GIP")
                                                                .build();
            when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    GetLastSnapshotResponse.newBuilder().setSnapshot(storedSnapshot).setSequence(50L).build()
            ));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(50L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(50L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, new GlobalIndexPosition(100L)),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then the snapshot is emitted first, followed by events sourced from the snapshot's position
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .assertNext(entry -> assertThat(entry.message()).isInstanceOf(SnapshotEventMessage.class))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            ArgumentCaptor<SourceEventsRequest> captor = ArgumentCaptor.forClass(SourceEventsRequest.class);
            verify(dcbEventChannel).source(captor.capture());
            assertThat(captor.getValue().getFromSequence()).isEqualTo(50L);
        }

        @Test
        void sourceWithBoundedSnapshotStrategyFallsBackToStartWhenSnapshotIsAfterMaximumPosition() {
            // given a snapshot at position 150, beyond the requested maximum position of 100
            io.axoniq.axonserver.grpc.event.dcb.Snapshot storedSnapshot =
                    io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                                .setName(qualifiedName.fullName())
                                                                .setVersion("0.0.1")
                                                                .putMetadata("__AxonFramework__:Position-Type", "GIP")
                                                                .build();
            when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    GetLastSnapshotResponse.newBuilder().setSnapshot(storedSnapshot).setSequence(150L).build()
            ));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, new GlobalIndexPosition(100L)),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then the snapshot is discarded, sourcing falls back to the very beginning
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            ArgumentCaptor<SourceEventsRequest> captor = ArgumentCaptor.forClass(SourceEventsRequest.class);
            verify(dcbEventChannel).source(captor.capture());
            assertThat(captor.getValue().getFromSequence()).isEqualTo(GlobalIndexPosition.toIndex(Position.START));
        }

        @Test
        void sourceWithBoundedSnapshotStrategyFallsBackToFullReconstructionWhenSnapshotLoadFails() {
            // given the snapshot load fails with something other than "not found"
            when(snapshotChannel.getLastSnapshot(any()))
                    .thenReturn(CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException()));
            when(sourcingStream.nextIfAvailable())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);
            when(sourcingStream.peek())
                    .thenReturn(SourceEventsResponse.newBuilder().setConsistencyMarker(0L).build(), null);

            SourcingCondition condition = SourcingCondition.conditionFor(
                    new SourcingStrategy.Snapshot(qualifiedName, identifier, new GlobalIndexPosition(100L)),
                    EventCriteria.havingTags("AGGREGATE_TYPE", identifier)
            );

            // when / then the failure does not propagate; sourcing falls back to full reconstruction instead
            StepVerifier.create(FluxUtils.of(testSubject.source(condition, null)))
                        .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                        .verifyComplete();

            ArgumentCaptor<SourceEventsRequest> captor = ArgumentCaptor.forClass(SourceEventsRequest.class);
            verify(dcbEventChannel).source(captor.capture());
            assertThat(captor.getValue().getFromSequence()).isEqualTo(GlobalIndexPosition.toIndex(Position.START));
        }
    }

    @Nested
    class CommitFailures {

        private DcbEventChannel.AppendEventsTransaction serverTransaction;

        @BeforeEach
        void setUp() {
            serverTransaction = mock(DcbEventChannel.AppendEventsTransaction.class);
            when(dcbEventChannel.startTransaction(any())).thenReturn(serverTransaction);
        }

        @Test
        void conditionNotMetIsReportedAsAConsistencyRejection() {
            // given a commit failing the way Axon Server reports an unmet append condition
            Throwable serverFailure = Status.CANCELLED
                    .withDescription("io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: "
                                             + "Consistency condition is not met.")
                    .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(serverFailure));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .hasCauseInstanceOf(AppendEventsTransactionRejectedException.class)
                    .rootCause()
                    .isSameAs(serverFailure);
        }

        @Test
        void aBrokenConnectionIsNotReportedAsAConsistencyRejection() {
            // given a commit failing because the connection to Axon Server is gone, so nobody knows the outcome
            Throwable transportFailure = Status.UNAVAILABLE
                    .withDescription("io exception")
                    .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(transportFailure));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .cause()
                    .isNotInstanceOf(AppendEventsTransactionRejectedException.class)
                    .isInstanceOf(EventStoreException.class)
                    .hasRootCause(transportFailure);
        }

        @Test
        void aCancelledCommitWithoutAServerDecisionIsNotReportedAsAConsistencyRejection() {
            // given a commit cancelled without Axon Server reporting a decision, the status it also uses
            // for an unmet append condition
            Throwable cancellation = Status.CANCELLED.withDescription("cancelled before receiving half close")
                                                     .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(cancellation));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .cause()
                    .isNotInstanceOf(AppendEventsTransactionRejectedException.class)
                    .isInstanceOf(EventStoreException.class)
                    .hasRootCause(cancellation);
        }

        @Test
        void conditionNotMetIsRecognisedWhenReportedDeeperInTheCauseChain() {
            // given the same rejection, wrapped by a layer that reports it as the cause rather than throwing it itself
            Throwable serverFailure = Status.CANCELLED
                    .withDescription("io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: "
                                             + "Consistency condition is not met.")
                    .asRuntimeException();
            Throwable wrapped = new IllegalStateException("Append transaction failed.",
                                                          new IllegalStateException("Commit failed.", serverFailure));
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(wrapped));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then the rejection is still recognised, two causes down
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .hasCauseInstanceOf(AppendEventsTransactionRejectedException.class)
                    .rootCause()
                    .isSameAs(serverFailure);
        }

        @Test
        void aTransportFailureCarryingAnEarlierRejectionAsItsCauseIsNotReportedAsAConsistencyRejection() {
            // given a lost connection whose cause chain happens to carry an earlier rejection, so the marker is
            // present even though this append was never decided on
            Throwable earlierRejection = Status.CANCELLED
                    .withDescription("io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: "
                                             + "Consistency condition is not met.")
                    .asRuntimeException();
            Throwable transportFailure = Status.UNAVAILABLE.withDescription("io exception")
                                                           .withCause(earlierRejection)
                                                           .asRuntimeException();
            when(serverTransaction.commit()).thenReturn(CompletableFuture.failedFuture(transportFailure));

            // when
            CompletableFuture<AppendEventsResponse> result = commit();

            // then the failure that terminated the call decides the outcome, so it is undetermined
            assertThatThrownBy(() -> result.orTimeout(5, TimeUnit.SECONDS).join())
                    .cause()
                    .isNotInstanceOf(AppendEventsTransactionRejectedException.class)
                    .isInstanceOf(EventStoreException.class);
        }

        @SuppressWarnings("unchecked")
        private CompletableFuture<AppendEventsResponse> commit() {
            EventMessage event = new GenericEventMessage(new MessageType(EVENT_NAME, "0.0.1"), "payload");
            EventStorageEngine.AppendTransaction<?> transaction = testSubject
                    .appendEvents(AppendCondition.none(),
                                  null,
                                  List.of(new GenericTaggedEventMessage<>(event, Set.of())))
                    .join();
            return (CompletableFuture<AppendEventsResponse>) transaction.commit();
        }
    }
}
