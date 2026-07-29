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
import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;

/**
 * Tests the {@link org.axonframework.eventsourcing.handler.SnapshottingEntityLifecycleHandler} with a
 * {@link PostgresqlEventStorageEngine}, which implements {@link SnapshotStore} directly.
 *
 * @author John Hendrikx
 */
@Testcontainers
class PostgresqlBackedSnapshotterIT extends SnapshottingEntityLifecycleHandlerTestSuite {

    @SuppressWarnings("resource")
    @Container
    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:16.2")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    private static DataSource dataSource;

    private PostgresqlEventStorageEngine engine;

    @BeforeAll
    static void buildDataSource() {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(CONTAINER.getJdbcUrl());
        config.setUsername(CONTAINER.getUsername());
        config.setPassword(CONTAINER.getPassword());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setAutoCommit(false);

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void closeDataSource() {
        ((HikariDataSource) dataSource).close();
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
        // Axon Server connector is on the classpath in this module; disable its enhancer so it
        // doesn't register an AxonServerConnectionManager that attempts to reach a non-existent server.
        registry.disableEnhancer(AxonServerConfigurationEnhancer.class);
        // Multi-tenancy is on the classpath too, and is Axon Server-backed; disable its enhancers for the
        // same reason.
        MultiTenancyConfigurationUtils.disableMultiTenancy(registry);

        registry.registerComponent(
                EventStorageEngine.class,
                c -> engine = new PostgresqlEventStorageEngine(
                        dataSource,
                        c.getComponent(EventConverter.class),
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
