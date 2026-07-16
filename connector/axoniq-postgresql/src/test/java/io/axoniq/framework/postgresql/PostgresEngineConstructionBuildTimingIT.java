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
        postgresContainer = new PostgreSQLContainer("postgres:16.2")
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
