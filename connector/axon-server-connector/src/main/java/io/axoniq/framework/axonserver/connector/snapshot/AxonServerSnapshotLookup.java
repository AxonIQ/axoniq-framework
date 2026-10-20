/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.snapshot;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.event.dcb.LegacySnapshotCandidate;
import io.axoniq.axonserver.grpc.event.dcb.SequencedSnapshot;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.AggregateSequenceNumberPosition;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.eventsourcing.snapshot.api.SnapshotMatch;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One snapshot lookup against Axon Server, translating the {@link SnapshotCandidate candidates} of a sourcing request
 * into the candidates of the request sent to Axon Server, and the snapshot Axon Server returns back into a
 * {@link SnapshotMatch}.
 * <p>
 * Each candidate is looked up under the key its name and the entity's identifier form. An aggregate-based candidate,
 * describing a snapshot such as one taken by Axon Framework 4, is looked up by the identifier alone, through the
 * request's legacy aggregate identifier. Such a snapshot is reported with its payload type as name and its revision
 * as version, where a snapshot without a revision has version {@value EventTypeResolver#MISSING_VERSION_DEFAULT}.
 * <p>
 * Shared by the {@link AxonServerSnapshotStore} and the snapshot-aware sourcing of the Axon Server event storage
 * engine, so both translate a snapshot lookup identically; not intended for other use.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
@Internal
public final class AxonServerSnapshotLookup {

    private final List<SnapshotCandidate> candidates;
    private final List<ByteString> keys;
    private final @Nullable SnapshotCandidate aggregateBasedCandidate;
    private final String aggregateIdentifier;

    AxonServerSnapshotLookup(List<SnapshotCandidate> candidates,
                             List<ByteString> keys,
                             @Nullable SnapshotCandidate aggregateBasedCandidate,
                             Object identifier) {
        this.candidates = List.copyOf(candidates);
        this.keys = List.copyOf(keys);
        this.aggregateBasedCandidate = aggregateBasedCandidate;
        this.aggregateIdentifier = identifier.toString();
    }

    /**
     * Returns the candidates to send to Axon Server, in order of preference.
     *
     * @return the candidates, never {@code null}
     */
    public List<io.axoniq.axonserver.grpc.event.dcb.SnapshotCandidate> requestCandidates() {
        List<io.axoniq.axonserver.grpc.event.dcb.SnapshotCandidate> requestCandidates = new ArrayList<>();

        for (int i = 0; i < candidates.size(); i++) {
            requestCandidates.add(io.axoniq.axonserver.grpc.event.dcb.SnapshotCandidate.newBuilder()
                                          .setKey(keys.get(i))
                                          .addAllAcceptableVersion(candidates.get(i).acceptableVersions())
                                          .build());
        }

        return requestCandidates;
    }

    /**
     * Returns the aggregate-based snapshot to look up when no candidate yields a snapshot: the aggregate identifier,
     * with the payload type and acceptable revisions of the aggregate-based candidate, where version
     * {@value EventTypeResolver#MISSING_VERSION_DEFAULT} stands for a snapshot without a revision.
     *
     * @return the legacy candidate of the request, or {@code null} when there is no aggregate-based candidate, which
     * leaves that lookup disabled
     */
    public @Nullable LegacySnapshotCandidate legacyCandidate() {
        if (aggregateBasedCandidate == null) {
            return null;
        }

        LegacySnapshotCandidate.Builder candidate =
                LegacySnapshotCandidate.newBuilder()
                                       .setAggregateIdentifier(aggregateIdentifier)
                                       .setPayloadType(aggregateBasedCandidate.qualifiedName().fullName());

        for (String version : aggregateBasedCandidate.acceptableVersions()) {
            candidate.addAcceptableRevision(
                    EventTypeResolver.MISSING_VERSION_DEFAULT.equals(version) ? "" : version
            );
        }

        return candidate.build();
    }

    /**
     * Translates a snapshot Axon Server found into a match, reporting it under the identity it was found as, whether
     * or not that identity is acceptable.
     * <p>
     * Used where the snapshot can no longer be passed over, such as at the head of a sourced stream that already
     * continues from it; the entity it is sourced for rejects a snapshot of a type it cannot use.
     *
     * @param found the snapshot Axon Server found, cannot be {@code null}
     * @return the match, never {@code null}
     */
    public SnapshotMatch foundMatch(SequencedSnapshot found) {
        if (found.getFromLegacyIdentifier()) {
            return new SnapshotMatch(new QualifiedName(found.getSnapshot().getName()), toSnapshot(found, true));
        }

        int index = keys.indexOf(found.getKey());
        QualifiedName name = index < 0
                ? new QualifiedName(found.getSnapshot().getName())
                : candidates.get(index).qualifiedName();

        return new SnapshotMatch(name, toSnapshot(found, false));
    }

    /**
     * Translates a snapshot Axon Server found into a match, but only when it is acceptable: when it was found under one
     * of the candidates' keys with one of that candidate's acceptable versions, or, for an aggregate-based snapshot,
     * when its payload type and revision are those of the aggregate-based candidate.
     *
     * @param found the snapshot Axon Server found, cannot be {@code null}
     * @return the match, or {@code null} if the snapshot is not acceptable
     */
    public @Nullable SnapshotMatch acceptedMatch(SequencedSnapshot found) {
        SnapshotMatch match = foundMatch(found);
        SnapshotCandidate candidate = found.getFromLegacyIdentifier()
                ? aggregateBasedCandidate
                : candidateFoundUnder(found.getKey());

        if (candidate == null
                || !candidate.qualifiedName().equals(match.qualifiedName())
                || !candidate.accepts(match.snapshot().version())) {
            return null;
        }

        return match;
    }

    /*
     * Returns the candidates with the keys they are looked up under, in order of preference, for looking them up one
     * by one where Axon Server cannot look them up at once.
     */
    List<SnapshotCandidate> candidates() {
        return candidates;
    }

    List<ByteString> keys() {
        return keys;
    }

    private @Nullable SnapshotCandidate candidateFoundUnder(ByteString key) {
        int index = keys.indexOf(key);

        return index < 0 ? null : candidates.get(index);
    }

    /*
     * Converts a found snapshot. Its sequence always follows the snapshot position convention, as Axon Server
     * translates the sequence of an aggregate-based snapshot. Snapshots stored by this framework record which kind of
     * position they hold in a reserved metadata entry, which is removed again; others hold a global index.
     */
    static Snapshot toSnapshot(SequencedSnapshot found, boolean aggregateBased) {
        io.axoniq.axonserver.grpc.event.dcb.Snapshot snapshot = found.getSnapshot();
        Map<String, String> metadata = new HashMap<>(snapshot.getMetadataMap());
        String positionType = metadata.remove(AxonServerSnapshotStore.POSITION_TYPE_KEY);
        Position position = "ASNP".equals(positionType)
                ? new AggregateSequenceNumberPosition(found.getSequence())
                : new GlobalIndexPosition(found.getSequence());
        String version = aggregateBased && snapshot.getVersion().isEmpty()
                ? EventTypeResolver.MISSING_VERSION_DEFAULT
                : snapshot.getVersion();

        return new Snapshot(position,
                            version,
                            snapshot.getPayload().toByteArray(),
                            Instant.ofEpochMilli(snapshot.getTimestamp()),
                            metadata);
    }
}
