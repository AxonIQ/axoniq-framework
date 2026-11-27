/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Map;

/**
 * Runs the {@link JpaWorkflowIndexProcessorTransactionTestSuite} on PostgreSQL.
 *
 * @author Stefan Dragisic
 */
class JpaWorkflowIndexProcessorTransactionIT extends JpaWorkflowIndexProcessorTransactionTestSuite {

    private static PostgreSQLContainer postgres;

    @BeforeAll
    static void startDatabase() {
        postgres = new PostgreSQLContainer("postgres:16.2-alpine");
        postgres.start();
    }

    @AfterAll
    static void stopDatabase() {
        postgres.stop();
    }

    @Override
    Map<String, Object> jdbcProperties() {
        return Map.of("jakarta.persistence.jdbc.driver", "org.postgresql.Driver",
                      "jakarta.persistence.jdbc.url", postgres.getJdbcUrl(),
                      "jakarta.persistence.jdbc.user", postgres.getUsername(),
                      "jakarta.persistence.jdbc.password", postgres.getPassword());
    }
}
