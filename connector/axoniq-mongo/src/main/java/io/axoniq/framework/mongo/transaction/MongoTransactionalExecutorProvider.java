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
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A {@link TransactionalExecutorProvider} implementation for MongoDB {@link ClientSession ClientSessions}, which
 * provides a {@link TransactionalExecutor}.
 * <p>
 * When a {@link ProcessingContext} is supplied, supplies the {@link ClientSessionExecutor} that has been bound to it,
 * typically by the {@link ClientSessionTransactionManager}. Operations executed through it run in the MongoDB
 * transaction of that processing context, and so are committed or rolled back together with everything else the
 * processing context does through the same session.
 * <p>
 * If no processing context is supplied, creates an executor that runs the supplied function with a session of its own,
 * which is closed afterwards. As that session has no transaction, every operation is only atomic on its own.
 * <p>
 * Example usage:
 * <pre>{@code
 * MongoTransactionalExecutorProvider provider = new MongoTransactionalExecutorProvider(mongoClient);
 * provider.getTransactionalExecutor(processingContext)
 *         .accept(session -> collection.insertOne(session, document));
 * }</pre>
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
public class MongoTransactionalExecutorProvider implements TransactionalExecutorProvider<ClientSession> {

    /**
     * The resource key for the {@link ClientSessionExecutor} supplier.
     */
    public static final ResourceKey<Supplier<ClientSessionExecutor>> SUPPLIER_KEY =
            ResourceKey.withLabel(ClientSessionExecutor.class.getSimpleName());

    private final Supplier<ClientSession> sessionSupplier;

    /**
     * Constructs a new instance, starting sessions on the given {@code mongoClient} when no processing context is
     * supplied.
     *
     * @param mongoClient the {@link MongoClient} to start sessions on
     */
    public MongoTransactionalExecutorProvider(MongoClient mongoClient) {
        this(Objects.requireNonNull(mongoClient, "mongoClient")::startSession);
    }

    /**
     * Constructs a new instance, obtaining sessions from the given {@code sessionSupplier} when no processing context
     * is supplied.
     *
     * @param sessionSupplier the supplier of a new {@link ClientSession} per invocation
     */
    public MongoTransactionalExecutorProvider(Supplier<ClientSession> sessionSupplier) {
        this.sessionSupplier = Objects.requireNonNull(sessionSupplier, "sessionSupplier");
    }

    @Nullable
    @Override
    public TransactionalExecutor<ClientSession> findTransactionalExecutor(
            @Nullable ProcessingContext processingContext
    ) {
        if (processingContext != null) {
            Supplier<ClientSessionExecutor> executorSupplier = processingContext.getResource(SUPPLIER_KEY);
            return executorSupplier == null ? null : executorSupplier.get();
        }

        return new TransactionalExecutor<>() {
            @Override
            public <R> CompletableFuture<R> apply(ThrowingFunction<ClientSession, R, Exception> function) {
                try (ClientSession session = sessionSupplier.get()) {
                    return CompletableFuture.completedFuture(function.apply(session));
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
        };
    }
}
