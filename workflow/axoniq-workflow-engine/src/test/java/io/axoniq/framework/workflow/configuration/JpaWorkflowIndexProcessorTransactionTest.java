/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import java.util.Map;
import java.util.UUID;

/**
 * Runs the {@link JpaWorkflowIndexProcessorTransactionTestSuite} on an in-memory HSQLDB database.
 *
 * @author Stefan Dragisic
 */
class JpaWorkflowIndexProcessorTransactionTest extends JpaWorkflowIndexProcessorTransactionTestSuite {

    // A database of its own for every test. Multi-version concurrency lets the segments write at the same time.
    private final String url = "jdbc:hsqldb:mem:" + UUID.randomUUID() + ";hsqldb.tx=mvcc";

    @Override
    Map<String, Object> jdbcProperties() {
        return Map.of("jakarta.persistence.jdbc.driver", "org.hsqldb.jdbcDriver",
                      "jakarta.persistence.jdbc.url", url,
                      "jakarta.persistence.jdbc.user", "sa",
                      "jakarta.persistence.jdbc.password", "");
    }
}
