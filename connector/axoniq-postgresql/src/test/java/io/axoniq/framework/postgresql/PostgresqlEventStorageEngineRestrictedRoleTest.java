/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.license.entitlement.EnforcingEntitlementManager;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link PostgresqlEventStorageEngine} starts and operates with a database role that
 * may read and write the schema's tables, but is not allowed to create anything in the schema, regardless of the
 * auto-commit setting of the connections handed out by its {@link javax.sql.DataSource}.
 *
 * @author John Hendrikx
 */
class PostgresqlEventStorageEngineRestrictedRoleTest {

    private static final String RESTRICTED_ROLE = "restricted_app";
    private static final String RESTRICTED_PASSWORD = "restricted";

    private static PostgreSQLContainer postgresContainer;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() throws SQLException {
        postgresContainer = new PostgreSQLContainer("postgres:16.2-alpine")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");
        postgresContainer.start();

        // given - the schema is provisioned up front by its owner, as a migration tool or DBA would
        try (HikariDataSource ownerDataSource = createDataSource(postgresContainer.getUsername(),
                                                                 postgresContainer.getPassword(),
                                                                 false)) {
            PostgresqlSchemaInitializer.initialize(ownerDataSource, SchemaInitialization.CREATE_IF_MISSING);

            try (
                Connection connection = ownerDataSource.getConnection();
                Statement statement = connection.createStatement()
            ) {
                statement.execute(
                    """
                    CREATE ROLE %1$s LOGIN PASSWORD '%2$s';
                    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
                    GRANT USAGE ON SCHEMA public TO %1$s;
                    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %1$s;
                    GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO %1$s;
                    """.formatted(RESTRICTED_ROLE, RESTRICTED_PASSWORD)
                );

                connection.commit();
            }
        }
    }

    @AfterAll
    static void stopContainer() {
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    static Stream<Arguments> schemaInitializationAndAutoCommit() {
        return Arrays.stream(SchemaInitialization.values())
                     .flatMap(si -> Stream.of(Arguments.of(si, true), Arguments.of(si, false)));
    }

    @ParameterizedTest(name = "{0}, autoCommit={1}")
    @MethodSource("schemaInitializationAndAutoCommit")
    void engineStartsAndStoresSnapshotsWithoutCreatePrivilegeOnAProvisionedSchema(
            SchemaInitialization schemaInitialization, boolean autoCommit
    ) {
        try (HikariDataSource restrictedDataSource = createDataSource(RESTRICTED_ROLE, RESTRICTED_PASSWORD,
                                                                      autoCommit)) {
            // when - the engine is constructed by a role that is not allowed to create anything
            PostgresqlEventStorageEngine engine = new PostgresqlEventStorageEngine(
                    restrictedDataSource,
                    new DelegatingEventConverter(new JacksonConverter()),
                    schemaInitialization,
                    Mockito.mock(EnforcingEntitlementManager.class)
            );

            try {
                // then - snapshots can still be stored and loaded through the provisioned table
                QualifiedName qualifiedName = new QualifiedName("com.example.Entity");
                String identifier = "entity-" + schemaInitialization + "-" + autoCommit;
                Snapshot snapshot = new Snapshot(new GlobalIndexPosition(1), "1", new byte[]{1, 2, 3},
                                                 Instant.parse("2026-01-01T00:00:00Z"), Map.of());

                engine.store(qualifiedName, identifier, snapshot, null).orTimeout(5, TimeUnit.SECONDS).join();

                assertThat(engine.load(qualifiedName, identifier, null).orTimeout(5, TimeUnit.SECONDS).join())
                        .extracting(Snapshot::version, Snapshot::position)
                        .containsExactly("1", new GlobalIndexPosition(1));
            }
            finally {
                engine.close();
            }
        }
    }

    private static HikariDataSource createDataSource(String username, String password, boolean autoCommit) {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(username);
        config.setPassword(password);
        config.setAutoCommit(autoCommit);

        return new HikariDataSource(config);
    }
}
