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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
 * <p>
 * Snapshots are written in the background, by a single writer that takes a connection of its own for each snapshot
 * and does not use the connection of any unit of work. {@link #store} therefore returns without waiting for a
 * connection. The future it returns completes when the snapshot was written, and completes exceptionally when writing
 * failed, or with a {@link RejectedExecutionException} when too many snapshots are waiting to be written.
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

    /*
     * The number of snapshots that can wait to be written. A snapshot is a cache of committed state, so when the
     * writer cannot keep up, snapshots that arrive while this many are waiting are rejected, instead of piling up
     * in memory and in line for connections ahead of real work. Each waiting snapshot holds its payload bytes.
     */
    static final int WRITE_QUEUE_CAPACITY = 128;

    private final DataSource dataSource;
    private final Converter converter;
    private final TransactionalExecutorProvider<Connection> transactionalExecutorProvider;

    /*
     * A single writer, so that at most one connection is in use for snapshots, and snapshots are written in the order
     * they were handed over. Each write takes a connection of its own and returns it right after.
     */
    private final ThreadPoolExecutor writer = new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(WRITE_QUEUE_CAPACITY),
        Thread.ofVirtual().name("PG-SnapshotWriter").factory()
    );

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

        /*
         * Everything that can fail on the caller's thread is done before the write is handed over: unsupported
         * position types are rejected right away, and the payload is converted now, as it can be the live entity,
         * which may still change once this method returns.
         */

        SnapshotRow row = new SnapshotRow(
            qualifiedName.fullName(),
            String.valueOf(identifier),
            snapshot.version(),
            toPositionType(snapshot.position()),
            GlobalIndexPosition.toIndex(snapshot.position()),
            converter.convert(snapshot.payload(), byte[].class),
            Timestamp.from(snapshot.timestamp()),
            MetadataSerializer.toJson(snapshot.metadata())
        );
        CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            writer.execute(() -> {
                try {
                    write(row).whenComplete((unused, e) -> {
                        if (e == null) {
                            result.complete(null);
                        }
                        else {
                            result.completeExceptionally(e);
                        }
                    });
                }
                catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            });
        }
        catch (RejectedExecutionException e) {
            result.completeExceptionally(new RejectedExecutionException(
                "Snapshot of " + qualifiedName + " with identifier " + identifier
                    + " was not written, as the snapshot writer is busy or closed", e
            ));
        }

        return result;
    }

    void close() {  // for testing purposes, to avoid junk exceptions
        writer.shutdown();

        try {
            if (!writer.awaitTermination(10, TimeUnit.SECONDS)) {
                writer.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            writer.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private CompletableFuture<Void> write(SnapshotRow row) {
        return transactionalExecutorProvider.getTransactionalExecutor(null).<Void>apply(connection -> {
            try (
                PreparedStatement del = connection.prepareStatement(SNAPSHOT_DELETE);
                PreparedStatement ins = connection.prepareStatement(SNAPSHOT_INSERT)
            ) {
                del.setString(1, row.qualifiedName());
                del.setString(2, row.identifier());
                del.executeUpdate();

                ins.setString(1, row.qualifiedName());
                ins.setString(2, row.identifier());
                ins.setString(3, row.version());
                ins.setString(4, row.positionType());
                ins.setLong(5, row.positionValue());
                ins.setBytes(6, row.payload());
                ins.setTimestamp(7, row.timestamp());
                ins.setString(8, row.metadata());
                ins.executeUpdate();

                return null;
            }
            catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to store snapshot for " + row.qualifiedName() + " with identifier " + row.identifier(), e);
            }
        });
    }

    private record SnapshotRow(String qualifiedName, String identifier, String version, String positionType,
                               long positionValue, byte[] payload, Timestamp timestamp, String metadata) {
    }

    @Override
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
