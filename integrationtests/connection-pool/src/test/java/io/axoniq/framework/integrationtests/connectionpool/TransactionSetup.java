/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.framework.integrationtests.connectionpool.PostgresServer.Database;

/**
 * A way of wiring a transaction manager, and everything in between, on top of a connection pool.
 *
 * @author John Hendrikx
 */
interface TransactionSetup {

    /**
     * Creates the environment.
     *
     * @param database        the database to connect to
     * @param userConnections the number of connections the pool should have available to application and framework
     *                        code, not counting the one the event storage engine reserves for its monitor thread
     * @param autoCommit      the auto-commit mode the pool hands out its connections in
     * @return the environment, never {@code null}
     * @throws Exception if the environment could not be set up
     */
    Environment create(Database database, int userConnections, boolean autoCommit) throws Exception;

    /**
     * Creates a pool configured to be as small as the scenarios need: the given number of connections for users plus
     * one that the event storage engine's monitor thread holds on to permanently, with a short wait for a connection
     * so that a deadlock surfaces as a failure and not as a hang.
     *
     * @param database        the database to connect to
     * @param userConnections the number of connections to leave to users
     * @param autoCommit      the auto-commit mode of the connections
     * @param poolName        the name of the pool
     * @return the pool, never {@code null}
     */
    static HikariDataSource pool(Database database, int userConnections, boolean autoCommit, String poolName) {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(database.url());
        config.setUsername(database.user());
        config.setPassword(database.password());
        config.setPoolName(poolName);
        config.setMaximumPoolSize(userConnections + 1);
        config.setMinimumIdle(userConnections + 1);
        config.setConnectionTimeout(2000);
        config.setAutoCommit(autoCommit);

        return new HikariDataSource(config);
    }
}
