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

package org.axonframework.modelling.saga.repository.jdbc;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.axonframework.common.jdbc.JdbcUtils.closeQuietly;

/**
 * Integration test class validating the {@link Oracle11SagaSqlSchema}.
 *
 * @author Joris van der Kallen
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
@Testcontainers
@Tag("slow")
@Tag("nightly")
class Oracle11SagaSqlSchemaIT {

    private static final String USERNAME = "test";
    private static final String PASSWORD = "test";

    // Oracle XE 11g matches the dialect under test, and its slim variant is a fraction of the size of the 21c
    // ":latest" image. 11g has no pluggable database, so the connection goes through the "xe" SID instead.
    @Container
    private static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer("gvenzl/oracle-xe:11-slim").usingSid();

    private Oracle11SagaSqlSchema testSubject;
    private Connection connection;
    private SagaSchema sagaSchema;

    @BeforeEach
    void setUp() throws SQLException {
        sagaSchema = new SagaSchema();
        testSubject = new Oracle11SagaSqlSchema(sagaSchema);
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
    void sql_createTableAssocValueEntry() throws Exception {
        // test passes if no exception is thrown
        testSubject.sql_createTableAssocValueEntry(connection)
                   .execute();
        connection.prepareStatement("SELECT * FROM " + sagaSchema.associationValueEntryTable())
                  .execute();

        connection.prepareStatement("DROP TABLE " + sagaSchema.associationValueEntryTable())
                  .execute();
        connection.prepareStatement("DROP SEQUENCE " + sagaSchema.associationValueEntryTable() + "_seq")
                  .execute();
    }

    @Test
    void sql_createTableSagaEntry() throws Exception {
        // test passes if no exception is thrown
        testSubject.sql_createTableSagaEntry(connection)
                   .execute();
        connection.prepareStatement("SELECT * FROM " + sagaSchema.sagaEntryTable())
                  .execute();

        connection.prepareStatement("DROP TABLE " + sagaSchema.sagaEntryTable())
                  .execute();
    }
}
