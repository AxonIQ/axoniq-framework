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
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
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
        assertThat(captor.getValue().getLegacyAggregateIdentifier()).isEmpty();
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

    private void stubChannel() {
        when(connection.snapshotChannel()).thenReturn(snapshotChannel);
    }

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
