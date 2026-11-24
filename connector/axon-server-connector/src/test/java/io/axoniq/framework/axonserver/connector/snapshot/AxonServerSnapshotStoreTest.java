/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.snapshot;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.grpc.event.dcb.FindLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.FindLastSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotResponse;
import io.axoniq.axonserver.grpc.event.dcb.SequencedSnapshot;
import io.grpc.Status;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.eventsourcing.snapshot.api.SnapshotMatch;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link AxonServerSnapshotStore#load} against an Axon Server that does not advertise the
 * {@code dcb:snapshot-aware-sourcing} capability, and against one that does.
 *
 * @author John Hendrikx
 */
@ExtendWith(MockitoExtension.class)
class AxonServerSnapshotStoreTest {

    private static final String CAPABILITY = "dcb:snapshot-aware-sourcing";
    private static final QualifiedName NAME = new QualifiedName("test-entity");
    private static final String IDENTIFIER = "entity-id";

    @Mock
    private AxonServerConnection connection;
    @Mock
    private SnapshotChannel snapshotChannel;

    private AxonServerSnapshotStore testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AxonServerSnapshotStore(connection, new DelegatingEventConverter(new JacksonConverter()));
    }

    @Test
    void shouldLoadWithASingleKeyThroughGetLastWhenTheServerHasNoCapabilities() {
        stubChannel();
        when(connection.supports(CAPABILITY)).thenReturn(false);
        when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                GetLastSnapshotResponse.newBuilder().setSnapshot(stored("GIP")).setSequence(42L).build()
        ));

        Snapshot result = load();

        assertThat(result.position()).isEqualTo(new GlobalIndexPosition(42L));
        ArgumentCaptor<GetLastSnapshotRequest> captor = ArgumentCaptor.forClass(GetLastSnapshotRequest.class);
        verify(snapshotChannel).getLastSnapshot(captor.capture());
        assertThat(captor.getValue().getKey()).isEqualTo(expectedKey());
        verify(snapshotChannel, never()).findLastSnapshot(any());
    }

    @Test
    void shouldLoadWithASingleAnyVersionCandidateThroughFindLastWhenTheServerAdvertisesTheCapability() {
        stubChannel();
        when(connection.supports(CAPABILITY)).thenReturn(true);
        when(snapshotChannel.findLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                findLastResponse("ASNP", 7L)
        ));

        Snapshot result = load();

        assertThat(result.position()).isEqualTo(new AggregateSequenceNumberPosition(7L));
        ArgumentCaptor<FindLastSnapshotRequest> captor = ArgumentCaptor.forClass(FindLastSnapshotRequest.class);
        verify(snapshotChannel).findLastSnapshot(captor.capture());
        assertThat(captor.getValue().getCandidateList()).hasSize(1);
        assertThat(captor.getValue().getCandidate(0).getKey()).isEqualTo(expectedKey());
        assertThat(captor.getValue().getCandidate(0).getAcceptableVersionList()).isEmpty();
        assertThat(captor.getValue().hasLegacyCandidate()).isFalse();
        verify(snapshotChannel, never()).getLastSnapshot(any());
    }

    @Test
    void shouldFallBackToGetLastWhenANodeAdvertisingTheCapabilityAnswersFindLastUnimplemented() {
        stubChannel();
        when(connection.supports(CAPABILITY)).thenReturn(true);
        when(snapshotChannel.findLastSnapshot(any())).thenReturn(
                CompletableFuture.failedFuture(Status.UNIMPLEMENTED.asRuntimeException())
        );
        when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                GetLastSnapshotResponse.newBuilder().setSnapshot(stored("GIP")).setSequence(9L).build()
        ));

        Snapshot result = load();

        assertThat(result.position()).isEqualTo(new GlobalIndexPosition(9L));
    }

    @Test
    void shouldReturnNullWhenFindLastFindsNoSnapshot() {
        stubChannel();
        when(connection.supports(CAPABILITY)).thenReturn(true);
        when(snapshotChannel.findLastSnapshot(any())).thenReturn(
                CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException())
        );

        assertThat(load()).isNull();
    }

    @Test
    void shouldReturnNullWhenGetLastFindsNoSnapshot() {
        stubChannel();
        when(connection.supports(CAPABILITY)).thenReturn(false);
        when(snapshotChannel.getLastSnapshot(any())).thenReturn(
                CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException())
        );

        assertThat(load()).isNull();
    }

    @Test
    void shouldReEvaluateTheCapabilityOnEveryLoad() {
        stubChannel();
        when(connection.supports(CAPABILITY)).thenReturn(true, false);
        when(snapshotChannel.findLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                findLastResponse("GIP", 1L)
        ));
        when(snapshotChannel.getLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                GetLastSnapshotResponse.newBuilder().setSnapshot(stored("GIP")).setSequence(2L).build()
        ));

        Snapshot first = load();
        Snapshot second = load();

        assertThat(first.position()).isEqualTo(new GlobalIndexPosition(1L));
        assertThat(second.position()).isEqualTo(new GlobalIndexPosition(2L));
    }

    @Nested
    class LoadWithCandidates {

        private static final QualifiedName FORMER_NAME = new QualifiedName("former-entity");
        private static final QualifiedName PAYLOAD_TYPE = new QualifiedName("com.example.EntityAggregate");

        private final List<SnapshotCandidate> candidates = List.of(
                new SnapshotCandidate(NAME, Set.of("0.0.1")),
                new SnapshotCandidate(FORMER_NAME, Set.of("1.0.0", "1.1.0"))
        );
        private final SnapshotCandidate aggregateBasedCandidate = new SnapshotCandidate(PAYLOAD_TYPE, Set.of("0.0.0"));

        @Test
        void shouldSendEveryCandidateWithItsAcceptableVersionsInOneFindLast() {
            // given
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(true);
            when(snapshotChannel.findLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    found(key(FORMER_NAME), false, FORMER_NAME.fullName(), "1.1.0")
            ));

            // when
            SnapshotMatch match = load(aggregateBasedCandidate);

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(FORMER_NAME);
            ArgumentCaptor<FindLastSnapshotRequest> captor = ArgumentCaptor.forClass(FindLastSnapshotRequest.class);
            verify(snapshotChannel).findLastSnapshot(captor.capture());
            assertThat(captor.getValue().getCandidateList())
                    .extracting(io.axoniq.axonserver.grpc.event.dcb.SnapshotCandidate::getKey)
                    .containsExactly(key(NAME), key(FORMER_NAME));
            assertThat(captor.getValue().getCandidate(1).getAcceptableVersionList())
                    .containsExactlyInAnyOrder("1.0.0", "1.1.0");
            assertThat(captor.getValue().getLegacyCandidate().getAggregateIdentifier()).isEqualTo(IDENTIFIER);
            assertThat(captor.getValue().getLegacyCandidate().getPayloadType()).isEqualTo(PAYLOAD_TYPE.fullName());
            assertThat(captor.getValue().getLegacyCandidate().getAcceptableRevisionList()).containsExactly("");
        }

        @Test
        void shouldNotLookUpByAggregateIdentifierWithoutAnAggregateBasedCandidate() {
            // given
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(true);
            when(snapshotChannel.findLastSnapshot(any())).thenReturn(
                    CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException())
            );

            // when
            SnapshotMatch match = load(null);

            // then
            assertThat(match).isNull();
            ArgumentCaptor<FindLastSnapshotRequest> captor = ArgumentCaptor.forClass(FindLastSnapshotRequest.class);
            verify(snapshotChannel).findLastSnapshot(captor.capture());
            assertThat(captor.getValue().hasLegacyCandidate()).isFalse();
        }

        @Test
        void shouldAcceptAnAggregateBasedSnapshotOfTheDeclaredPayloadTypeAndRevision() {
            // given an aggregate-based snapshot without a revision
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(true);
            when(snapshotChannel.findLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    found(ByteString.copyFromUtf8("sn-" + IDENTIFIER), true, PAYLOAD_TYPE.fullName(), "")
            ));

            // when
            SnapshotMatch match = load(aggregateBasedCandidate);

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(PAYLOAD_TYPE);
            assertThat(match.snapshot().version()).isEqualTo("0.0.0");
            assertThat(match.snapshot().position()).isEqualTo(new GlobalIndexPosition(12L));
        }

        @Test
        void shouldRejectAnAggregateBasedSnapshotOfAnotherPayloadType() {
            // given a snapshot of another aggregate type stored under the same identifier
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(true);
            when(snapshotChannel.findLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    found(ByteString.copyFromUtf8("sn-" + IDENTIFIER), true, "com.example.OtherAggregate", "")
            ));

            // when / then
            assertThat(load(aggregateBasedCandidate)).isNull();
        }

        @Test
        void shouldRejectAnAggregateBasedSnapshotOfAnUndeclaredRevision() {
            // given
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(true);
            when(snapshotChannel.findLastSnapshot(any())).thenReturn(CompletableFuture.completedFuture(
                    found(ByteString.copyFromUtf8("sn-" + IDENTIFIER), true, PAYLOAD_TYPE.fullName(), "2")
            ));

            // when / then
            assertThat(load(aggregateBasedCandidate)).isNull();
        }

        @Test
        void shouldAskEachCandidateInTurnThroughGetLastWhenTheServerHasNoCapabilities() {
            // given a first candidate without a snapshot, and a second with one of an acceptable version
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(false);
            when(snapshotChannel.getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key(NAME)).build()))
                    .thenReturn(CompletableFuture.failedFuture(Status.NOT_FOUND.asRuntimeException()));
            when(snapshotChannel.getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key(FORMER_NAME)).build()))
                    .thenReturn(CompletableFuture.completedFuture(getLastResponse(FORMER_NAME, "1.0.0")));

            // when
            SnapshotMatch match = load(aggregateBasedCandidate);

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(FORMER_NAME);
            verify(snapshotChannel, never()).findLastSnapshot(any());
        }

        @Test
        void shouldPassOverAGetLastSnapshotOfAnUnacceptableVersion() {
            // given a first candidate whose latest snapshot was written by a newer version of the application
            stubChannel();
            when(connection.supports(CAPABILITY)).thenReturn(false);
            when(snapshotChannel.getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key(NAME)).build()))
                    .thenReturn(CompletableFuture.completedFuture(getLastResponse(NAME, "9.0.0")));
            when(snapshotChannel.getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key(FORMER_NAME)).build()))
                    .thenReturn(CompletableFuture.completedFuture(getLastResponse(FORMER_NAME, "1.0.0")));

            // when
            SnapshotMatch match = load(aggregateBasedCandidate);

            // then
            assertThat(match).isNotNull();
            assertThat(match.qualifiedName()).isEqualTo(FORMER_NAME);
        }

        private SnapshotMatch load(SnapshotCandidate aggregateBased) {
            return testSubject.load(candidates, aggregateBased, IDENTIFIER, null)
                              .orTimeout(5, TimeUnit.SECONDS)
                              .join();
        }

        private ByteString key(QualifiedName name) {
            return testSubject.snapshotKey(name, IDENTIFIER);
        }

        private static FindLastSnapshotResponse found(ByteString key, boolean aggregateBased, String name,
                                                      String version) {
            return FindLastSnapshotResponse.newBuilder()
                                           .setSnapshot(SequencedSnapshot.newBuilder()
                                                                         .setKey(key)
                                                                         .setSequence(12L)
                                                                         .setFromLegacyIdentifier(aggregateBased)
                                                                         .setSnapshot(stored(name, version)))
                                           .build();
        }

        private static GetLastSnapshotResponse getLastResponse(QualifiedName name, String version) {
            return GetLastSnapshotResponse.newBuilder().setSnapshot(stored(name.fullName(), version)).setSequence(3L)
                                          .build();
        }

        private static io.axoniq.axonserver.grpc.event.dcb.Snapshot stored(String name, String version) {
            return io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                               .setName(name)
                                                               .setVersion(version)
                                                               .setTimestamp(Instant.now().toEpochMilli())
                                                               .build();
        }
    }

    private void stubChannel() {
        when(connection.snapshotChannel()).thenReturn(snapshotChannel);
    }

    // temporary: exercises the deprecated single-name load Axon Framework keeps until Axoniq Framework has migrated
    @SuppressWarnings("removal")
    private Snapshot load() {
        return testSubject.load(NAME, IDENTIFIER, null).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private ByteString expectedKey() {
        return testSubject.snapshotKey(NAME, IDENTIFIER);
    }

    private FindLastSnapshotResponse findLastResponse(String positionType, long sequence) {
        return FindLastSnapshotResponse.newBuilder()
                                       .setSnapshot(SequencedSnapshot.newBuilder()
                                                                     .setKey(expectedKey())
                                                                     .setSequence(sequence)
                                                                     .setSnapshot(stored(positionType)))
                                       .build();
    }

    private static io.axoniq.axonserver.grpc.event.dcb.Snapshot stored(String positionType) {
        return io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                                                           .setName(NAME.fullName())
                                                           .setVersion("0.0.1")
                                                           .setTimestamp(Instant.now().toEpochMilli())
                                                           .putMetadata("__AxonFramework__:Position-Type", positionType)
                                                           .build();
    }
}
