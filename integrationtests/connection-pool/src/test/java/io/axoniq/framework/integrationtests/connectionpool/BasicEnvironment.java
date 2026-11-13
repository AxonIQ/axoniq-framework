/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import javax.sql.DataSource;

/**
 * A plain implementation of {@link Environment} assembled from its parts.
 *
 * @author John Hendrikx
 */
final class BasicEnvironment implements Environment {

    private final DataSource engineDataSource;
    private final IntSupplier connectionsInUse;
    private final TransactionManager transactionManager;
    private final BiConsumer<String, Object[]> userSql;
    private final Consumer<Runnable> outerTransaction;
    private final List<AutoCloseable> closeables;

    /**
     * Constructs a new instance.
     *
     * @param engineDataSource   the data source for the event storage engine
     * @param connectionsInUse   reports how many connections of the pool behind that data source are in use
     * @param transactionManager the transaction manager for units of work
     * @param userSql            how application code runs SQL in the current transaction
     * @param outerTransaction   runs work inside a transaction of its own
     * @param closeables         everything to close, in order, when the environment is closed
     */
    BasicEnvironment(DataSource engineDataSource,
                     IntSupplier connectionsInUse,
                     TransactionManager transactionManager,
                     BiConsumer<String, Object[]> userSql,
                     Consumer<Runnable> outerTransaction,
                     List<AutoCloseable> closeables) {
        this.engineDataSource = engineDataSource;
        this.connectionsInUse = connectionsInUse;
        this.transactionManager = transactionManager;
        this.userSql = userSql;
        this.outerTransaction = outerTransaction;
        this.closeables = closeables;
    }

    @Override
    public DataSource engineDataSource() {
        return engineDataSource;
    }

    @Override
    public int connectionsInUse() {
        return connectionsInUse.getAsInt();
    }

    @Override
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    @Override
    public void userSql(String sql, Object... args) {
        userSql.accept(sql, args);
    }

    @Override
    public void runInOuterTransaction(Runnable work) {
        outerTransaction.accept(work);
    }

    @Override
    public void close() {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            }
            catch (Exception e) {
                // best effort cleanup of a test environment
            }
        }
    }
}
