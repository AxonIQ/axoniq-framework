/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.mongo;

import com.mongodb.client.ClientSession;
import io.axoniq.framework.mongo.transaction.ClientSessionExecutor;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.extension.spring.messaging.unitofwork.SpringTransactionManager;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import static org.axonframework.conversion.CachingSupplier.of;

/**
 * Transaction manager to be used with Spring and MongoDB. It wraps Spring's {@link MongoTransactionManager}, so the
 * processing lifecycle runs in a Spring managed MongoDB transaction, exactly like it did in Axon Framework 4.
 * <p>
 * Every processing context is given the {@link ClientSession} of that Spring managed transaction, from where the
 * {@link MongoTransactionalExecutorProvider} hands it to the components that take part in the transaction, such as the
 * {@link io.axoniq.framework.mongo.token.MongoTokenStore} and the dead-letter queue. As the transaction is the Spring
 * managed one, all code that obtains its database through {@link MongoDatabaseUtils#getDatabase} takes part in it as
 * well. That is what the {@link SpringMongoTemplate} and Spring Data's {@code MongoTemplate} and repositories do, so
 * they join the transaction without passing a session.
 * <p>
 * The session is looked up lazily, on first use, on the thread that runs the processing lifecycle. This manager
 * therefore requires that a processing context is invoked on a single thread, as Spring binds the transaction to the
 * thread.
 * <p>
 * Spring Data does not expose the session of the transaction it binds to the thread. This manager reads it from the
 * bound {@code MongoResourceHolder} reflectively.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class SpringMongoTransactionManager extends SpringTransactionManager {

    private static final String GET_SESSION_METHOD = "getSession";

    private final MongoTransactionManager manager;

    /**
     * Creates a new {@code SpringMongoTransactionManager} using the supplied {@link MongoTransactionManager}.
     *
     * @param manager the {@link MongoTransactionManager} used to instantiate a {@code SpringMongoTransactionManager}
     *                instance
     */
    public SpringMongoTransactionManager(MongoTransactionManager manager) {
        this(manager, null);
    }

    /**
     * Creates a new {@code SpringMongoTransactionManager} using the supplied {@link MongoTransactionManager}, starting
     * its transactions with the given {@code transactionDefinition}.
     *
     * @param manager               the {@link MongoTransactionManager} used to instantiate a
     *                              {@code SpringMongoTransactionManager} instance
     * @param transactionDefinition the {@link TransactionDefinition} to start transactions with, defaults to a
     *                              {@code DefaultTransactionDefinition} when {@code null}
     */
    public SpringMongoTransactionManager(MongoTransactionManager manager,
                                         @Nullable TransactionDefinition transactionDefinition) {
        super(manager, transactionDefinition);
        this.manager = manager;
    }

    @Override
    public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
        super.attachToProcessingLifecycle(processingLifecycle);
        processingLifecycle.runOnPreInvocation(
                context -> context.putResource(MongoTransactionalExecutorProvider.SUPPLIER_KEY,
                                               of(() -> new ClientSessionExecutor(this::currentSession)))
        );
    }

    private ClientSession currentSession() {
        Object holder = TransactionSynchronizationManager.getResource(manager.getResourceFactory());
        if (holder == null) {
            throw new IllegalStateException(
                    "No Spring managed MongoDB transaction is bound to the current thread. The "
                            + SpringMongoTransactionManager.class.getSimpleName() + " requires that the processing "
                            + "context is invoked on the thread that started the transaction."
            );
        }
        try {
            Method getSession = holder.getClass().getDeclaredMethod(GET_SESSION_METHOD);
            getSession.setAccessible(true);
            ClientSession session = (ClientSession) getSession.invoke(holder);
            return Objects.requireNonNull(session, "The Spring managed MongoDB transaction has no session.");
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(
                    "Could not retrieve the session of the Spring managed MongoDB transaction from ["
                            + holder.getClass().getName() + "]. This Spring Data MongoDB version is not supported.", e
            );
        }
    }
}
