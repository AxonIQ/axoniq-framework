/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotCandidate;
import org.axonframework.eventsourcing.snapshot.api.SnapshotMatch;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;

/**
 * Internal PostgreSQL-backed implementation of {@link SnapshotStore}, used exclusively by
 * {@link PostgresqlEventStorageEngine}.
 * <p>
 * Not intended for direct use - snapshot access is exposed through the engine, which implements
 * {@link SnapshotStore} and delegates to this class for direct snapshot storage and retrieval.
 * This ensures the snapshot table is always co-located with the event tables on the same
 * {@link DataSource}, which is what allows the engine to source with a snapshot in a single
 * round trip: rather than delegating to this class, it reads the snapshot and the tail events
 * in one query against both tables.
 * <p>
 * Only {@link GlobalIndexPosition} is supported as a position type. Snapshots with any other
 * position type are rejected.
 *
 * @author John Hendrikx
 * @since 5.2.0
 */
@Internal
class PostgresqlSnapshotStore implements SnapshotStore {

    /**
     * Deletes all snapshots for a given entity, regardless of version.
     *
     * <li>Parameter 1 {@code String}: the qualified name
     * <li>Parameter 2 {@code String}: the identifier
     */
    private static final String SNAPSHOT_DELETE =
        """
        DELETE FROM snapshots WHERE qualified_name = ? AND identifier = ?
        """;

    /**
     * Inserts a snapshot.
     *
     * <li>Parameter 1 {@code String}: the qualified name
     * <li>Parameter 2 {@code String}: the identifier
     * <li>Parameter 3 {@code String}: the entity version
     * <li>Parameter 4 {@code String}: the position type discriminator
     * <li>Parameter 5 {@code long}: the position value
     * <li>Parameter 6 {@code byte[]}: the serialized payload
     * <li>Parameter 7 {@code Timestamp}: the snapshot timestamp
     * <li>Parameter 8 {@code String}: the metadata in JSON format
     */
    private static final String SNAPSHOT_INSERT =
        """
        INSERT INTO snapshots (qualified_name, identifier, version, position_type, position_value, payload, timestamp, metadata)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?::json)
        """;

    /**
     * Selects the latest snapshot for a given entity by highest position value.
     *
     * <li>Parameter 1 {@code String}: the qualified name
     * <li>Parameter 2 {@code String}: the identifier
     */
    private static final String SNAPSHOT_SELECT =
        """
        SELECT position_type, position_value, version, payload, timestamp, metadata
          FROM snapshots
          WHERE qualified_name = ? AND identifier = ?
          ORDER BY position_value DESC
          LIMIT 1
        """;

    /**
     * Selects the snapshot for a given entity among the given candidates: the first candidate, by rank, with a
     * snapshot of an acceptable version wins, and of its acceptable snapshots the one with the highest position
     * value. The candidates are bound by {@link #bindCandidates}.
     *
     * <li>Parameter 1 {@code int[]}: the rank of each candidate entry
     * <li>Parameter 2 {@code String[]}: the qualified name of each candidate entry
     * <li>Parameter 3 {@code String[]}: an acceptable version of each candidate entry, or {@code null} for any
     * <li>Parameter 4 {@code String}: the identifier
     */
    private static final String SNAPSHOT_SELECT_CANDIDATES =
        """
        SELECT s.qualified_name, s.position_type, s.position_value, s.version, s.payload, s.timestamp, s.metadata
          FROM snapshots s
          JOIN unnest(?::int4[], ?::varchar[], ?::varchar[]) AS c (rank, qualified_name, version)
            ON s.qualified_name = c.qualified_name AND (c.version IS NULL OR s.version = c.version)
          WHERE s.identifier = ?
          ORDER BY c.rank, s.position_value DESC
          LIMIT 1
        """;

    private final DataSource dataSource;
    private final Converter converter;
    private final TransactionalExecutorProvider<Connection> transactionalExecutorProvider;

    /**
     * Constructs a new instance. The {@code snapshots} table is part of the engine's schema, so it is created
     * or validated by {@link PostgresqlSchemaInitializer} rather than by this class.
     *
     * @param dataSource a data source to connect to PostgreSQL, cannot be {@code null}
     * @param converter  a converter for snapshot payload serialization, cannot be {@code null}
     * @param transactionalExecutorProvider provides the connection executor to use, cannot be {@code null}
     */
    PostgresqlSnapshotStore(DataSource dataSource, Converter converter,
                            TransactionalExecutorProvider<Connection> transactionalExecutorProvider) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.transactionalExecutorProvider = Objects.requireNonNull(transactionalExecutorProvider, "transactionalExecutorProvider");
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot,
                                         @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(snapshot, "snapshot");

        // Validate and extract position eagerly to fail fast on unsupported position types
        String positionType = toPositionType(snapshot.position());
        long positionValue = GlobalIndexPosition.toIndex(snapshot.position());
        byte[] payload = converter.convert(snapshot.payload(), byte[].class);
        String metadata = MetadataSerializer.toJson(snapshot.metadata());

        return transactionalExecutorProvider.getTransactionalExecutor(null).<Void>apply(connection -> {
            try (
                PreparedStatement del = connection.prepareStatement(SNAPSHOT_DELETE);
                PreparedStatement ins = connection.prepareStatement(SNAPSHOT_INSERT)
            ) {
                del.setString(1, qualifiedName.fullName());
                del.setString(2, String.valueOf(identifier));
                del.executeUpdate();

                ins.setString(1, qualifiedName.fullName());
                ins.setString(2, String.valueOf(identifier));
                ins.setString(3, snapshot.version());
                ins.setString(4, positionType);
                ins.setLong(5, positionValue);
                ins.setBytes(6, payload);
                ins.setTimestamp(7, Timestamp.from(snapshot.timestamp()));
                ins.setString(8, metadata);
                ins.executeUpdate();

                return null;
            }
            catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to store snapshot for " + qualifiedName + " with identifier " + identifier, e);
            }
        });
    }

    @Override
    public CompletableFuture<@Nullable SnapshotMatch> load(List<SnapshotCandidate> candidates, Object identifier,
                                                           @Nullable ProcessingContext context) {
        List<SnapshotCandidate> candidateList = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
        Objects.requireNonNull(identifier, "identifier");

        if (candidateList.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }

        return transactionalExecutorProvider.getTransactionalExecutorPreferringIndependent(context).apply(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(SNAPSHOT_SELECT_CANDIDATES)) {
                int parameterIndex = bindCandidates(connection, ps, 1, candidateList);

                ps.setString(parameterIndex, String.valueOf(identifier));

                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }

                    QualifiedName qualifiedName = new QualifiedName(rs.getString(1));
                    String positionType = rs.getString(2);
                    long positionValue = rs.getLong(3);
                    String version = rs.getString(4);
                    byte[] payload = rs.getBytes(5);
                    Instant timestamp = rs.getTimestamp(6).toInstant();
                    Map<String, String> metadata = MetadataSerializer.fromJson(rs.getString(7));

                    return new SnapshotMatch(
                        qualifiedName,
                        new Snapshot(toPosition(positionType, positionValue), version, payload, timestamp, metadata)
                    );
                }
            }
            catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to load snapshot for " + candidateList + " with identifier " + identifier, e);
            }
        });
    }

    /*
     * Binds the given candidates to the three array parameters starting at the given index, as one entry per
     * acceptable version, so that joining against them matches a snapshot of any acceptable version of any
     * candidate while keeping each entry's rank. Returns the index of the parameter following them.
     */
    static int bindCandidates(Connection connection, PreparedStatement statement, int parameterIndex,
                              List<SnapshotCandidate> candidates) throws SQLException {
        List<Integer> ranks = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<@Nullable String> versions = new ArrayList<>();

        for (int rank = 0; rank < candidates.size(); rank++) {
            SnapshotCandidate candidate = candidates.get(rank);
            String name = candidate.qualifiedName().fullName();

            /*
             * Temporary: a candidate without versions accepts any version, which Axon Framework allows only while it
             * still offers its deprecated version-less SourcingStrategy.Snapshot constructor. It is bound as a single
             * entry with a null version. Remove once Axon Framework rejects an empty version set again.
             */

            if (candidate.acceptableVersions().isEmpty()) {
                ranks.add(rank);
                names.add(name);
                versions.add(null);
            }

            for (String version : candidate.acceptableVersions()) {
                ranks.add(rank);
                names.add(name);
                versions.add(version);
            }
        }

        statement.setArray(parameterIndex++, connection.createArrayOf("int4", ranks.toArray()));
        statement.setArray(parameterIndex++, connection.createArrayOf("varchar", names.toArray()));
        statement.setArray(parameterIndex++, connection.createArrayOf("varchar", versions.toArray()));

        return parameterIndex;
    }

    /*
     * Temporary: Axon Framework keeps this single-name load as a deprecated abstract method until Axoniq Framework
     * has migrated. It deliberately carries no @Override, so Axon Framework can remove the method first without
     * breaking this class; remove it afterwards.
     */
    @Deprecated(since = "5.5.0", forRemoval = true)
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier,
                                                      @Nullable ProcessingContext context) {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        Objects.requireNonNull(identifier, "identifier");

        return transactionalExecutorProvider.getTransactionalExecutorPreferringIndependent(context).apply(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(SNAPSHOT_SELECT)) {
                ps.setString(1, qualifiedName.fullName());
                ps.setString(2, String.valueOf(identifier));

                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }

                    String positionType = rs.getString(1);
                    long positionValue = rs.getLong(2);
                    String version = rs.getString(3);
                    byte[] payload = rs.getBytes(4);
                    Instant timestamp = rs.getTimestamp(5).toInstant();
                    Map<String, String> metadata = MetadataSerializer.fromJson(rs.getString(6));

                    return new Snapshot(toPosition(positionType, positionValue), version, payload, timestamp, metadata);
                }
            }
            catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to load snapshot for " + qualifiedName + " with identifier " + identifier, e);
            }
        });
    }

    private static String toPositionType(Position position) {
        return switch (position) {
            case GlobalIndexPosition ignored -> "GIP";
            default -> throw new IllegalArgumentException(
                    "Unsupported position type for PostgreSQL snapshot store: " + position.getClass().getSimpleName());
        };
    }

    private static Position toPosition(String positionType, long positionValue) {
        return switch (positionType) {
            case "GIP" -> new GlobalIndexPosition(positionValue);
            default -> throw new IllegalArgumentException("Unknown position type in snapshot: " + positionType);
        };
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("dataSource", dataSource);
        descriptor.describeProperty("converter", converter);
    }
}
