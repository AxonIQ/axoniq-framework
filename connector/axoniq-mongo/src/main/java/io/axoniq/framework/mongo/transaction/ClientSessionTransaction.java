/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.transaction;

import com.mongodb.client.ClientSession;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Transaction} that wraps a started MongoDB transaction on a {@link ClientSession}.
 * <p>
 * As a MongoDB transaction is only reached through its session, this transaction exposes the {@link #session()} that
 * everything that has to take part in it needs to use, and a {@link #executor()} that hands that session out like the
 * {@link ClientSessionExecutor} does. {@link #commit() Committing} and {@link #rollback() rolling back} conclude the
 * transaction and close the session. A transaction is concluded once: any further commit or rollback is ignored.
 * <p>
 * Obtain an instance through {@link ClientSessionTransactionManager#startTransaction()}.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
public class ClientSessionTransaction implements Transaction {

    private static final Logger logger = LoggerFactory.getLogger(ClientSessionTransaction.class);

    private final ClientSession session;
    private final AtomicBoolean concluded = new AtomicBoolean(false);

    /**
     * Starts a MongoDB transaction on the given {@code session}.
     * <p>
     * The session is closed when the transaction is concluded, or right away when the transaction cannot be started.
     *
     * @param session the {@link ClientSession} to start the transaction on
     */
    public ClientSessionTransaction(ClientSession session) {
        this.session = Objects.requireNonNull(session, "session");
        try {
            session.startTransaction();
        } catch (RuntimeException e) {
            session.close();
            throw e;
        }
    }

    /**
     * Returns the {@link ClientSession} the transaction runs on. Every operation that has to take part in the
     * transaction has to be passed this session.
     *
     * @return the session of this transaction
     */
    public ClientSession session() {
        return session;
    }

    /**
     * Returns a {@link TransactionalExecutor} that hands out the {@link #session() session} of this transaction. It
     * never commits, aborts or closes the session, which stays with this transaction.
     *
     * @return an executor on the session of this transaction
     */
    public TransactionalExecutor<ClientSession> executor() {
        return new ClientSessionExecutor(() -> session);
    }

    @Override
    public void commit() {
        if (concluded.compareAndSet(false, true)) {
            try {
                session.commitTransaction();
            } finally {
                session.close();
            }
        }
    }

    @Override
    public void rollback() {
        if (concluded.compareAndSet(false, true)) {
            try {
                if (session.hasActiveTransaction()) {
                    session.abortTransaction();
                }
            } catch (RuntimeException e) {
                logger.warn("Aborting the MongoDB transaction failed. The server aborts it when the session times out.",
                            e);
            } finally {
                session.close();
            }
        }
    }
}
