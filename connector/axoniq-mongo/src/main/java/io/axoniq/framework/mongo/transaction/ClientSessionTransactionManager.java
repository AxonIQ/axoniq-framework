/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.transaction;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import org.axonframework.conversion.CachingSupplier;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A {@link TransactionManager} implementation that manages MongoDB transactions through a {@link ClientSession} per
 * processing lifecycle.
 * <p>
 * When attached to a {@link ProcessingLifecycle}, this manager binds a {@link ClientSessionExecutor} to the
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext ProcessingContext} under the
 * {@link MongoTransactionalExecutorProvider#SUPPLIER_KEY}. The first operation that uses it starts a session and a
 * transaction. The transaction is committed when the processing lifecycle commits, and aborted when it fails, after
 * which the session is closed. Everything the processing lifecycle does through that session, such as storing a
 * tracking token and updating a projection, is therefore applied or discarded in one go.
 * <p>
 * A processing lifecycle that never touches the executor never starts a session. Outside a processing lifecycle,
 * {@link #startTransaction()} starts a transaction whose {@link ClientSessionTransaction#session() session} the caller
 * has to pass to every operation that must take part in it.
 * <p>
 * MongoDB only supports multi-document transactions on a replica set or sharded cluster. A {@code ClientSession} is not
 * safe for concurrent use, so {@link #requiresSameThreadInvocations()} returns {@code true}.
 * <p>
 * Example usage:
 * <pre>{@code
 * MessagingConfigurer.create()
 *                    .componentRegistry(cr -> cr.registerComponent(
 *                            TransactionManager.class,
 *                            c -> new ClientSessionTransactionManager(mongoClient)
 *                    ));
 * }</pre>
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
public class ClientSessionTransactionManager implements TransactionManager {

    private final Supplier<ClientSession> sessionSupplier;

    /**
     * Constructs a new instance, starting sessions on the given {@code mongoClient}.
     *
     * @param mongoClient the {@link MongoClient} to start sessions on
     */
    public ClientSessionTransactionManager(MongoClient mongoClient) {
        this(Objects.requireNonNull(mongoClient, "mongoClient")::startSession);
    }

    /**
     * Constructs a new instance, obtaining sessions from the given {@code sessionSupplier}.
     *
     * @param sessionSupplier the supplier of a new {@link ClientSession} per processing lifecycle
     */
    public ClientSessionTransactionManager(Supplier<ClientSession> sessionSupplier) {
        this.sessionSupplier = Objects.requireNonNull(sessionSupplier, "sessionSupplier");
    }

    /**
     * Starts a MongoDB transaction on a new session.
     * <p>
     * As a MongoDB transaction is only reached through its session, the returned {@link ClientSessionTransaction}
     * exposes the {@link ClientSessionTransaction#session() session} that every operation that has to take part in it
     * must be passed. Operations that are not passed that session are not part of the transaction. Committing or
     * rolling back the transaction closes the session.
     * <p>
     * Prefer {@link #attachToProcessingLifecycle(ProcessingLifecycle)} within a processing lifecycle, as that binds the
     * session to the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext ProcessingContext}.
     *
     * @return the started {@link ClientSessionTransaction}
     */
    @Override
    public ClientSessionTransaction startTransaction() {
        return new ClientSessionTransaction(sessionSupplier.get());
    }

    @Override
    public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
        processingLifecycle.runOnPreInvocation(context -> {
            AtomicReference<ClientSessionTransaction> transactionReference = new AtomicReference<>();
            ClientSessionExecutor executor = new ClientSessionExecutor(() -> {
                ClientSessionTransaction transaction = startTransaction();
                transactionReference.set(transaction);
                return transaction.session();
            });

            context.putResource(MongoTransactionalExecutorProvider.SUPPLIER_KEY, CachingSupplier.of(() -> executor));
            context.runOnCommit(p -> {
                ClientSessionTransaction transaction = transactionReference.get();
                if (transaction != null) {
                    transaction.commit();
                }
            });
            context.onError((p, phase, e) -> {
                ClientSessionTransaction transaction = transactionReference.get();
                if (transaction != null) {
                    transaction.rollback();
                }
            });
        });
    }

    @Override
    public boolean requiresSameThreadInvocations() {
        return true;
    }
}
