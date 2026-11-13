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
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.TimeUnit;

class PostgresEngineConstructionBuildTimingIT {

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;

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

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void buildCompletesWithinReasonableTimeAfterConstructingRealEngine() {
        PostgresqlEventStorageEngine engine = new PostgresqlEventStorageEngine(
                dataSource,
                new DelegatingEventConverter(new JacksonConverter(JsonMapper.builder().build()))
        );

        try {
            EventSourcingConfigurer.create().build();
        }
        finally {
            engine.close();
        }
    }
}
