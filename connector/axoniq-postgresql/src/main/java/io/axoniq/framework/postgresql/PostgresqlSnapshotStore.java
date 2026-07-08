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

package io.axoniq.framework.postgresql;

import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.Position;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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

    private static final Executor EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

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

    private final DataSource dataSource;
    private final Converter converter;

    /**
     * Constructs a new instance, creating the {@code snapshots} table if it does not yet exist.
     *
     * @param dataSource a data source to connect to PostgreSQL, cannot be {@code null}
     * @param converter  a converter for snapshot payload serialization, cannot be {@code null}
     */
    PostgresqlSnapshotStore(DataSource dataSource, Converter converter) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.converter = Objects.requireNonNull(converter, "converter");

        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                  qualified_name VARCHAR     NOT NULL,
                  identifier     VARCHAR     NOT NULL,
                  position_type  VARCHAR     NOT NULL,
                  position_value INT8        NOT NULL,
                  version        VARCHAR     NOT NULL,
                  payload        BYTEA       NOT NULL,
                  timestamp      TIMESTAMPTZ NOT NULL,
                  metadata       JSON        NOT NULL,
                  PRIMARY KEY (qualified_name, identifier, version)
                )
                """
            );

            connection.commit();
        }
        catch (SQLException e) {
            throw new IllegalStateException("Could not initialize " + getClass().getSimpleName(), e);
        }
    }

    @Override
    public CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot) {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(snapshot, "snapshot");

        // Validate and extract position eagerly to fail fast on unsupported position types
        String positionType = toPositionType(snapshot.position());
        long positionValue = GlobalIndexPosition.toIndex(snapshot.position());
        byte[] payload = converter.convert(snapshot.payload(), byte[].class);
        String metadata = MetadataSerializer.toJson(snapshot.metadata());

        return CompletableFuture.runAsync(() -> {
            try (
                Connection connection = dataSource.getConnection();
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

                connection.commit();
            }
            catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to store snapshot for " + qualifiedName + " with identifier " + identifier, e);
            }
        }, EXECUTOR);
    }

    @Override
    public CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier) {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        Objects.requireNonNull(identifier, "identifier");

        return CompletableFuture.supplyAsync(() -> {
            try (
                Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(SNAPSHOT_SELECT)
            ) {
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
        }, EXECUTOR);
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
}
