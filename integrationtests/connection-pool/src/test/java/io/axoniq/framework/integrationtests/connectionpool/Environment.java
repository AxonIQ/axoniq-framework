/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

import javax.sql.DataSource;

/**
 * A wired-up transaction setup on top of a small connection pool: the transaction manager that units of work use,
 * the data source the event storage engine uses, and a way for application code to run SQL in the transaction of the
 * calling thread.
 *
 * @author John Hendrikx
 */
interface Environment extends AutoCloseable {

    /**
     * Returns the data source handed to the event storage engine.
     *
     * @return the data source, never {@code null}
     */
    DataSource engineDataSource();

    /**
     * Returns the number of connections of the pool behind {@link #engineDataSource()} that are currently in use.
     *
     * @return the number of connections in use
     */
    int connectionsInUse();

    /**
     * Returns the transaction manager that units of work are bound to.
     *
     * @return the transaction manager, never {@code null}
     */
    TransactionManager transactionManager();

    /**
     * Runs an update statement the way application code would: using the framework's transactional facilities of
     * this setup, so that it takes part in the transaction of the calling thread when there is one.
     *
     * @param sql  the statement
     * @param args the statement arguments
     */
    void userSql(String sql, Object... args);

    /**
     * Runs work inside a transaction of its own on the calling thread, the way an application does that calls the
     * framework from a transactional service method.
     *
     * @param work the work to run, which can start units of work
     */
    void runInOuterTransaction(Runnable work);

    @Override
    void close();
}
