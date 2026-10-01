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
