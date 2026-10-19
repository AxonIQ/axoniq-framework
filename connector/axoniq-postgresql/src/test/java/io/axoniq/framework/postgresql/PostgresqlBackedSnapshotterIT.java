/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.license.entitlement.EnforcingEntitlementManager;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.eventsourcing.SnapshottingEntityLifecycleHandlerTestSuite;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Tests the {@link org.axonframework.eventsourcing.handler.SnapshottingEntityLifecycleHandler} with a
 * {@link PostgresqlEventStorageEngine}, which implements {@link SnapshotStore} directly.
 *
 * @author John Hendrikx
 */
class PostgresqlBackedSnapshotterIT extends SnapshottingEntityLifecycleHandlerTestSuite {

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;

    private PostgresqlEventStorageEngine engine;

    @BeforeAll
    @SuppressWarnings("resource")
    static void buildDataSource() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2-alpine")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");

        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void closeDataSource() {
        dataSource.close();

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @AfterEach
    void closeEngine() {
        if (engine != null) {
            engine.close();
            engine = null;
        }
    }

    @Override
    protected void registerComponents(ComponentRegistry registry) {
        registry.registerComponent(
                EventStorageEngine.class,
                c -> engine = new PostgresqlEventStorageEngine(
                        dataSource,
                        c.getComponent(EventConverter.class),
                        SchemaInitialization.CREATE_IF_MISSING,
                        Mockito.mock(EnforcingEntitlementManager.class)
                )
        );

        registry.registerComponent(SnapshotStore.class, c -> {
            if (engine == null) {
                c.getComponent(EventStorageEngine.class);
            }

            return engine;
        });
    }
}
