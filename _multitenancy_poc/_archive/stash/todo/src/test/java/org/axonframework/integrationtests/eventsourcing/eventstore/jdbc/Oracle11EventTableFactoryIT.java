/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.integrationtests.eventsourcing.eventstore.jdbc;

import org.axonframework.messaging.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.messaging.eventsourcing.eventstore.jdbc.Oracle11EventTableFactory;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.axonframework.common.io.IOUtils.closeQuietly;

/**
 * Integration test class validating the {@link Oracle11EventTableFactory}.
 *
 * @author Joris van der Kallen
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
@Testcontainers
@Tag("slow")
@Tag("nightly")
class Oracle11EventTableFactoryIT {

    private static final String USERNAME = "test";
    private static final String PASSWORD = "test";

    @Container
    private static final OracleContainer ORACLE_CONTAINER = new OracleContainer("gvenzl/oracle-xe");

    private Oracle11EventTableFactory testSubject;
    private Connection connection;
    private EventSchema eventSchema;

    @BeforeEach
    void setUp() throws SQLException {
        testSubject = new Oracle11EventTableFactory();
        eventSchema = new EventSchema();
        Properties properties = new Properties();
        properties.setProperty("user", USERNAME);
        properties.setProperty("password", PASSWORD);
        //Disable oracle.jdbc.timezoneAsRegion as when on true GHA fails to run this test due to missing region-info
        properties.setProperty("oracle.jdbc.timezoneAsRegion", "false");
        connection = DriverManager.getConnection(ORACLE_CONTAINER.getJdbcUrl(), properties);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(connection);
    }

    @Test
    void createDomainEventTable() throws Exception {
        // test passes if no exception is thrown
        testSubject.createDomainEventTable(connection, eventSchema)
                   .execute();
        connection.prepareStatement("SELECT * FROM " + eventSchema.domainEventTable())
                  .execute();

        connection.prepareStatement("DROP TABLE " + eventSchema.domainEventTable())
                  .execute();
        connection.prepareStatement("DROP SEQUENCE " + eventSchema.domainEventTable() + "_seq")
                  .execute();
    }

    @Test
    void createSnapshotEventTable() throws Exception {
        // test passes if no exception is thrown
        testSubject.createSnapshotEventTable(connection, eventSchema)
                   .execute();
        connection.prepareStatement("SELECT * FROM " + eventSchema.snapshotTable())
                  .execute();

        connection.prepareStatement("DROP TABLE " + eventSchema.snapshotTable())
                  .execute();
    }
}
