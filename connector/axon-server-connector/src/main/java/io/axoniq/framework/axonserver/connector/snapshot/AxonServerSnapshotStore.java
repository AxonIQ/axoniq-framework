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
import io.axoniq.axonserver.grpc.event.dcb.AddSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.FindLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.GetLastSnapshotRequest;
import io.axoniq.axonserver.grpc.event.dcb.SnapshotCandidate;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Objects.requireNonNull;

/**
 * An Axon Server based implementation of {@link SnapshotStore}.
 * <p>
 * This store stores snapshots in Axon Server and is thread-safe.
 *
 * @author John Hendrikx
 * @since 5.1.0
 */
public class AxonServerSnapshotStore implements SnapshotStore {

    /**
     * The Axon Server capability advertising {@code FindLast} and {@code SourceWithSnapshot}, which take ordered
     * {@code SnapshotCandidate}s. A node without it only offers {@code GetLast}, which takes a single key.
     */
    public static final String SNAPSHOT_AWARE_SOURCING_CAPABILITY = "dcb:snapshot-aware-sourcing";

    private static final String POSITION_TYPE_KEY = "__AxonFramework__:Position-Type";  // reserved key in metadata
    private static final ByteString SNAPSHOT_KEY_SEPARATOR = ByteString.copyFrom(new byte[]{0});

    private final AxonServerConnection connection;
    private final Converter converter;

    /**
     * Creates a new instance.
     *
     * @param connection a {@link AxonServerConnection}, cannot be {@code null}
     * @param converter a {@link Converter} for identifiers and payloads, cannot be {@code null}
     */
    public AxonServerSnapshotStore(AxonServerConnection connection, Converter converter) {
        this.connection = Objects.requireNonNull(connection, "The connection parameter must not be null.");
        this.converter = Objects.requireNonNull(converter, "The converter parameter must not be null.");
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName, "The qualifiedName parameter must not be null.");
        Objects.requireNonNull(identifier, "The identifier parameter must not be null.");
        Objects.requireNonNull(snapshot, "The snapshot parameter must not be null.");

        ByteString key = snapshotKey(qualifiedName, identifier);
        ByteString data = converter.convert(snapshot.payload(), byte[].class) instanceof byte[] ba ? ByteString.copyFrom(ba) : ByteString.EMPTY;

        return connection.snapshotChannel()
            .addSnapshot(AddSnapshotRequest.newBuilder()
                .setKey(key)
                .setSequence(switch(snapshot.position()) {
                    case GlobalIndexPosition gip -> GlobalIndexPosition.toIndex(snapshot.position());
                    case AggregateSequenceNumberPosition asnp -> AggregateSequenceNumberPosition.toSequenceNumber(snapshot.position());
                    default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
                })
                .setPrune(true)  // this is the standard behavior as documented in the SnapshotStore interface
                .setSnapshot(io.axoniq.axonserver.grpc.event.dcb.Snapshot.newBuilder()
                    .setName(qualifiedName.fullName())
                    .setVersion(snapshot.version())
                    .setTimestamp(snapshot.timestamp().toEpochMilli())  // Axon Server expects milliseconds since epoch
                    .setPayload(data)
                    .putAllMetadata(snapshot.metadata())
                    .putMetadata(POSITION_TYPE_KEY, switch(snapshot.position()) {
                        case GlobalIndexPosition gip -> "GIP";
                        case AggregateSequenceNumberPosition asnp -> "ASNP";
                        default -> throw new IllegalArgumentException("Unsupported position type: " + snapshot.position());
                    })
                )
                .build()
            )
            .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName, "The qualifiedName parameter must not be null.");
        Objects.requireNonNull(identifier, "The identifier parameter must not be null.");

        ByteString key = snapshotKey(qualifiedName, identifier);

        /*
         * Note, even though the server is documented to not return a snapshot when none is available, it actually
         * answers with an error, which is handled in the exceptionally block.
         */

        CompletableFuture<LastSnapshot> found = connection.supports(SNAPSHOT_AWARE_SOURCING_CAPABILITY)
                ? findLast(key)
                : getLast(key);

        return found
            .thenApply(last -> {
                io.axoniq.axonserver.grpc.event.dcb.Snapshot snapshot = last.snapshot();

                Map<String, String> metadata = new HashMap<>(snapshot.getMetadataMap());
                String positionType = metadata.remove(POSITION_TYPE_KEY);  // remove this key, so user doesn't notice
                Position position = switch(positionType) {
                    case "GIP" -> new GlobalIndexPosition(last.sequence());
                    case "ASNP" -> new AggregateSequenceNumberPosition(last.sequence());
                    case null, default -> throw new IllegalArgumentException("Unexpected position type: " + positionType);
                };

                return new Snapshot(
                    position,
                    snapshot.getVersion(),
                    snapshot.getPayload().toByteArray(),
                    Instant.ofEpochMilli(snapshot.getTimestamp()),
                    metadata
                );
            })
            .exceptionally(e -> {
                while (e instanceof CompletionException) {
                    e = e.getCause();
                }

                if (e instanceof StatusRuntimeException sre && (sre.getStatus().getCode() == Status.Code.CANCELLED || sre.getStatus().getCode()  == Status.Code.NOT_FOUND)) {
                    return null;  // Although CANCELLED is a bit odd, this is the current behavior when snapshot is not found.
                }

                throw new CompletionException("Snapshot loading failed for %s with identifier %s".formatted(qualifiedName.toString(), identifier.toString()), e);
            });
    }

    private CompletableFuture<LastSnapshot> getLast(ByteString key) {
        return connection.snapshotChannel()
            .getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key).build())
            .thenApply(response -> new LastSnapshot(response.getSnapshot(), response.getSequence()));
    }

    /*
     * Falls back to getLast when a node that advertised the capability still answers UNIMPLEMENTED, which can happen
     * when the connection moved to a less capable node in between.
     */
    private CompletableFuture<LastSnapshot> findLast(ByteString key) {
        FindLastSnapshotRequest request =
                FindLastSnapshotRequest.newBuilder()
                                       .addCandidate(SnapshotCandidate.newBuilder().setKey(key))
                                       .build();

        return connection.snapshotChannel()
            .findLastSnapshot(request)
            .thenApply(response -> new LastSnapshot(response.getSnapshot().getSnapshot(),
                                                    response.getSnapshot().getSequence()))
            .exceptionallyCompose(e -> isUnimplemented(e) ? getLast(key) : CompletableFuture.failedFuture(e));
    }

    private static boolean isUnimplemented(Throwable e) {
        Throwable cause = e;

        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }

        return cause instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.UNIMPLEMENTED;
    }

    private record LastSnapshot(io.axoniq.axonserver.grpc.event.dcb.Snapshot snapshot, long sequence) {

    }

    /**
     * Converts the given {@code name} and {@code identifier} into the Axon Server snapshot key wire format.
     *
     * @param name       the {@link QualifiedName} defining the snapshotted entity's type
     * @param identifier the identifier of the snapshotted entity
     * @return the snapshot key bytes for the given {@code name} and {@code identifier}
     */
    public ByteString snapshotKey(QualifiedName name, Object identifier) {
        byte[] nameAsBytes = requireNonNull(
                converter.convert(name.name(), byte[].class),
                "Converted name must not be null."
        );
        byte[] idAsBytes = requireNonNull(
                converter.convert(identifier, byte[].class),
                "Converted identifier must not be null."
        );
        return ByteString.copyFrom(nameAsBytes)
                         .concat(SNAPSHOT_KEY_SEPARATOR)
                         .concat(ByteString.copyFrom(idAsBytes));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("connection", connection);
        descriptor.describeProperty("converter", converter);
    }
}
