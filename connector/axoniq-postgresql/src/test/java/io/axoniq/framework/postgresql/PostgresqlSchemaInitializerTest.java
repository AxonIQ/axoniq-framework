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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link PostgresqlSchemaInitializer}.
 *
 * @author John Hendrikx
 */
class PostgresqlSchemaInitializerTest {

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");
        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void stopContainer() {
        dataSource.close();

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @BeforeEach
    void dropSchema() throws SQLException {
        // A full drop, not just DELETE, so each test can exercise "first-time install" again;
        // installTypeTagMigration() skips its own work once the trigger already exists.
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            connection.setAutoCommit(false);

            statement.execute(
                """
                DROP TRIGGER IF EXISTS axon_events_write_type_tag ON events;
                DROP TRIGGER IF EXISTS axon_tags_validate_type ON tags;
                DROP FUNCTION IF EXISTS axon_write_type_tag();
                DROP FUNCTION IF EXISTS axon_validate_type_tag();
                DROP TABLE IF EXISTS tags;
                DROP TABLE IF EXISTS consistency_tags;
                DROP TABLE IF EXISTS events;
                DROP SEQUENCE IF EXISTS events_monotonic_seq;
                """
            );

            connection.commit();
        }
    }

    @Test
    void installTypeTagMigrationBackfillsPreExistingEventsAndKeepsWritingGoingForward() throws SQLException {
        // given - a schema as an older version of the engine (before "__T" or "type_version"
        // existed) would have left it: "type" holds the combined "qualifiedName#version" string,
        // and the migration/trigger has never run. createSchema() now creates "type_version"
        // directly for fresh installs, so it is dropped again here to simulate that older shape.
        PostgresqlSchemaInitializer.createSchema(dataSource);

        dropTypeVersionColumn();

        // and - events already appended by that older version, with a combined "type" value and
        // no "__T" tag at all, since nothing wrote one for them.
        insertLegacyEventDirectly(-1, "com.example.EventA#1.0.0");
        insertLegacyEventDirectly(-2, "com.example.EventB#2.0.0");

        // when - the engine is "upgraded": the migration runs for the first time.
        PostgresqlSchemaInitializer.installTypeTagMigration(dataSource);

        // then - "type" was split into qualified name and version, and the backfill assigned the
        // correct (qualified name only) type tag to both pre-existing events.
        assertThat(queryTypeTag(-1)).isEqualTo("com.example.EventA");
        assertThat(queryTypeTag(-2)).isEqualTo("com.example.EventB");
        assertThat(queryTypeVersion(-1)).isEqualTo("1.0.0");
        assertThat(queryTypeVersion(-2)).isEqualTo("2.0.0");

        // and - the trigger now writes the type tag for any newly-inserted event too.
        insertEventDirectly(-3, "com.example.EventC", "1.0.0");
        assertThat(queryTypeTag(-3)).isEqualTo("com.example.EventC");
    }

    @Test
    void initializeOnAlreadyMigratedSchemaDoesNotErrorOrDuplicateTags() throws SQLException {
        PostgresqlSchemaInitializer.initialize(dataSource);

        insertEventDirectly(-1, "com.example.EventA", "1.0.0");

        // Running it again (e.g. a second engine instance starting up against an already-migrated
        // database) must be a no-op, not fail or create a duplicate "__T" row.
        PostgresqlSchemaInitializer.initialize(dataSource);

        assertThat(queryTypeTag(-1)).isEqualTo("com.example.EventA");
    }

    private void dropTypeVersionColumn() throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            connection.setAutoCommit(false);

            statement.execute("ALTER TABLE events DROP COLUMN type_version");

            connection.commit();
        }
    }

    private void insertEventDirectly(long globalIndex, String type, String typeVersion) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO events (global_index, timestamp, metadata, identifier, type, type_version) VALUES (?, now(), '{}', ?, ?, ?)"
            )
        ) {
            connection.setAutoCommit(false);

            ps.setLong(1, globalIndex);
            ps.setString(2, "identifier-" + globalIndex);
            ps.setString(3, type);
            ps.setString(4, typeVersion);
            ps.execute();

            connection.commit();
        }
    }

    /**
     * Inserts an event with a combined "qualifiedName#version" {@code type} value and no
     * {@code type_version} at all, simulating a row written before that column existed. Only
     * usable after {@link #dropTypeVersionColumn()}.
     */
    private void insertLegacyEventDirectly(long globalIndex, String combinedType) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO events (global_index, timestamp, metadata, identifier, type) VALUES (?, now(), '{}', ?, ?)"
            )
        ) {
            connection.setAutoCommit(false);

            ps.setLong(1, globalIndex);
            ps.setString(2, "identifier-" + globalIndex);
            ps.setString(3, combinedType);
            ps.execute();

            connection.commit();
        }
    }

    private String queryTypeTag(long globalIndex) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(
                "SELECT value FROM tags WHERE global_index = ? AND key = '__T'"
            )
        ) {
            ps.setLong(1, globalIndex);

            try (ResultSet resultSet = ps.executeQuery()) {
                resultSet.next();

                return resultSet.getString(1);
            }
        }
    }

    private String queryTypeVersion(long globalIndex) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(
                "SELECT type_version FROM events WHERE global_index = ?"
            )
        ) {
            ps.setLong(1, globalIndex);

            try (ResultSet resultSet = ps.executeQuery()) {
                resultSet.next();

                return resultSet.getString(1);
            }
        }
    }
}
