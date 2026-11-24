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
import io.axoniq.axonserver.grpc.event.dcb.LegacySnapshotCandidate;
import io.axoniq.axonserver.grpc.event.dcb.SequencedSnapshot;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.eventsourcing.snapshot.api.SnapshotMatch;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

    static final String POSITION_TYPE_KEY = "__AxonFramework__:Position-Type";  // reserved key in metadata
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
    public CompletableFuture<@Nullable SnapshotMatch> load(List<SnapshotCandidate> candidates, Object identifier,
                                                           @Nullable ProcessingContext context) {
        return load(candidates, null, identifier, context);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@code aggregateBasedCandidate} is only looked up when the connected Axon Server node supports looking
     * snapshots up by several candidates at once; an older node is asked for each candidate in turn instead, which
     * cannot reach an aggregate-based snapshot.
     */
    @Override
    public CompletableFuture<@Nullable SnapshotMatch> load(List<SnapshotCandidate> candidates,
                                                           @Nullable SnapshotCandidate aggregateBasedCandidate,
                                                           Object identifier,
                                                           @Nullable ProcessingContext context) {
        AxonServerSnapshotLookup lookup = lookupFor(candidates, aggregateBasedCandidate, identifier);
        CompletableFuture<@Nullable SnapshotMatch> found = connection.supports(SNAPSHOT_AWARE_SOURCING_CAPABILITY)
                ? findLast(lookup)
                : getLastOfEach(lookup, 0);

        /*
         * Even though Axon Server is documented to not return a snapshot when none is available, it actually answers
         * with an error, which is handled here.
         */

        return found.exceptionally(e -> {
            if (isNotFound(e)) {
                return null;
            }

            throw new CompletionException(
                    "Snapshot loading failed for %s with identifier %s".formatted(candidates, identifier), unwrap(e)
            );
        });
    }

    /*
     * Temporary: Axon Framework keeps this single-name load as a deprecated abstract method until Axoniq Framework
     * has migrated. It deliberately carries no @Override, so Axon Framework can remove the method first without
     * breaking this class; remove it afterwards.
     */
    @Deprecated(since = "5.5.0", forRemoval = true)
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName, "The qualifiedName parameter must not be null.");

        return load(List.of(new SnapshotCandidate(qualifiedName, Set.of())), identifier, context)
                .thenApply(match -> match == null ? null : match.snapshot());
    }

    /**
     * Creates the lookup of a snapshot of the entity with the given {@code identifier} under the given candidates,
     * which translates them into the candidates of a request to Axon Server, and the snapshot it returns back.
     *
     * @param candidates              the candidates to look the snapshot up under, in order of preference, cannot be
     *                                {@code null} or empty
     * @param aggregateBasedCandidate the aggregate-based snapshot to look up when no candidate yields one, or
     *                                {@code null} when there is none to look up
     * @param identifier              the identifier of the snapshotted entity, cannot be {@code null}
     * @return the lookup, never {@code null}
     * @throws NullPointerException     if {@code candidates} or {@code identifier} is {@code null}, or
     *                                  {@code candidates} contains {@code null}
     * @throws IllegalArgumentException if {@code candidates} is empty
     */
    public AxonServerSnapshotLookup lookupFor(List<SnapshotCandidate> candidates,
                                              @Nullable SnapshotCandidate aggregateBasedCandidate,
                                              Object identifier) {
        List<SnapshotCandidate> candidateList =
                List.copyOf(Objects.requireNonNull(candidates, "The candidates parameter must not be null."));
        Objects.requireNonNull(identifier, "The identifier parameter must not be null.");

        if (candidateList.isEmpty()) {
            throw new IllegalArgumentException("The candidates parameter must not be empty.");
        }

        List<ByteString> keys = new ArrayList<>();

        for (SnapshotCandidate candidate : candidateList) {
            keys.add(snapshotKey(candidate.qualifiedName(), identifier));
        }

        return new AxonServerSnapshotLookup(candidateList, keys, aggregateBasedCandidate, identifier);
    }

    /*
     * Falls back to asking for each candidate in turn when a node that advertised the capability still answers
     * UNIMPLEMENTED, which can happen when the connection moved to a less capable node in between.
     */
    private CompletableFuture<@Nullable SnapshotMatch> findLast(AxonServerSnapshotLookup lookup) {
        FindLastSnapshotRequest.Builder request =
                FindLastSnapshotRequest.newBuilder().addAllCandidate(lookup.requestCandidates());
        LegacySnapshotCandidate legacyCandidate = lookup.legacyCandidate();

        if (legacyCandidate != null) {
            request.setLegacyCandidate(legacyCandidate);
        }

        return connection.snapshotChannel()
                         .findLastSnapshot(request.build())
                         .<@Nullable SnapshotMatch>thenApply(response -> lookup.acceptedMatch(response.getSnapshot()))
                         .exceptionallyCompose(e -> isUnimplemented(e)
                                 ? getLastOfEach(lookup, 0)
                                 : CompletableFuture.failedFuture(e));
    }

    /*
     * Asks for the latest snapshot under each candidate's key in turn, until one has an acceptable version.
     */
    private CompletableFuture<@Nullable SnapshotMatch> getLastOfEach(AxonServerSnapshotLookup lookup, int index) {
        if (index == lookup.candidates().size()) {
            return CompletableFuture.completedFuture(null);
        }

        ByteString key = lookup.keys().get(index);

        return connection.snapshotChannel()
                         .getLastSnapshot(GetLastSnapshotRequest.newBuilder().setKey(key).build())
                         .<@Nullable SnapshotMatch>thenApply(response -> lookup.acceptedMatch(
                                 SequencedSnapshot.newBuilder()
                                                  .setKey(key)
                                                  .setSequence(response.getSequence())
                                                  .setSnapshot(response.getSnapshot())
                                                  .build()
                         ))
                         .exceptionally(e -> {
                             if (isNotFound(e)) {
                                 return null;
                             }

                             throw e instanceof CompletionException ce ? ce : new CompletionException(e);
                         })
                         .thenCompose(match -> match == null
                                 ? getLastOfEach(lookup, index + 1)
                                 : CompletableFuture.completedFuture(match));
    }

    /*
     * Although CANCELLED is a bit odd, it is what Axon Server answers when no snapshot is found, besides NOT_FOUND.
     */
    private static boolean isNotFound(Throwable e) {
        return unwrap(e) instanceof StatusRuntimeException sre
                && (sre.getStatus().getCode() == Status.Code.CANCELLED
                || sre.getStatus().getCode() == Status.Code.NOT_FOUND);
    }

    private static boolean isUnimplemented(Throwable e) {
        return unwrap(e) instanceof StatusRuntimeException sre
                && sre.getStatus().getCode() == Status.Code.UNIMPLEMENTED;
    }

    private static Throwable unwrap(Throwable e) {
        Throwable cause = e;

        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }

        return cause;
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
