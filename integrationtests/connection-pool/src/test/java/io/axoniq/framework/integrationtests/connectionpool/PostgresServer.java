/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a single PostgreSQL container shared by all tests in the JVM, handing out a fresh, empty database per
 * call to {@link #newDatabase()} so tests cannot influence each other.
 *
 * @author John Hendrikx
 */
final class PostgresServer {

    private static final AtomicInteger DATABASE_COUNTER = new AtomicInteger();
    private static final PostgreSQLContainer CONTAINER = startContainer();

    private PostgresServer() {
    }

    @SuppressWarnings("resource")
    private static PostgreSQLContainer startContainer() {
        PostgreSQLContainer container = new PostgreSQLContainer("postgres:16.2-alpine")
                .withDatabaseName("postgres")
                .withUsername("test")
                .withPassword("test");

        container.start();

        return container;
    }

    /**
     * Creates a fresh, empty database.
     *
     * @return a handle to the new database, never {@code null}
     * @throws SQLException if the database could not be created
     */
    static Database newDatabase() throws SQLException {
        String name = "pooltest_" + DATABASE_COUNTER.incrementAndGet();

        try (Connection connection = DriverManager.getConnection(
                CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword()
        );
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + name);
        }

        return new Database(
                CONTAINER.getJdbcUrl().replace("/postgres", "/" + name),
                CONTAINER.getUsername(),
                CONTAINER.getPassword(),
                name
        );
    }

    /**
     * A database on the shared server, which also offers a way to inspect it without going through any connection
     * pool under test.
     *
     * @param url      the JDBC URL of the database
     * @param user     the user name
     * @param password the password
     * @param name     the name of the database
     */
    record Database(String url, String user, String password, String name) {

        /**
         * Runs a statement on a connection that is not part of any pool under test.
         *
         * @param sql the statement to run
         * @throws SQLException if the statement fails
         */
        void execute(String sql) throws SQLException {
            try (Connection connection = DriverManager.getConnection(url, user, password);
                 Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }

        /**
         * Runs a query returning a single number on a connection that is not part of any pool under test.
         *
         * @param sql the query to run
         * @return the number in the first column of the first row
         * @throws SQLException if the query fails
         */
        long queryLong(String sql) throws SQLException {
            try (Connection connection = DriverManager.getConnection(url, user, password);
                 PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();

                return resultSet.getLong(1);
            }
        }

        /**
         * Counts the sessions of this database which are inside a transaction without running a statement.
         *
         * @return the number of sessions that are idle in transaction
         * @throws SQLException if the query fails
         */
        long idleInTransactionSessions() throws SQLException {
            return queryLong(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = '" + name
                            + "' AND state LIKE 'idle in transaction%'"
            );
        }
    }
}
