/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.jpa.EntityManagerExecutor;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Runs every unit of work of the workflow processor in a JPA transaction of its own, and binds its
 * {@link EntityManager} to the processing context, as the transaction manager of a Spring application with JPA does.
 * <p>
 * The {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore} and the
 * {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex} then write a checkpoint in that one transaction, so
 * the index commits its changes, and the hibernation hints of hibernated workflows, together with the token. A unit of
 * work only opens its transaction when the token store or the index first asks for it, so a batch that writes nothing
 * holds no database connection.
 *
 * @author Allard Buijze
 */
final class ProcessorTransactionManager implements TransactionManager {

    private final EntityManagerFactory entityManagerFactory;

    /**
     * Creates a transaction manager opening the transactions of the given {@code entityManagerFactory}.
     *
     * @param entityManagerFactory the entity manager factory of the database holding the token store and index
     */
    ProcessorTransactionManager(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = Objects.requireNonNull(entityManagerFactory,
                                                           "The EntityManagerFactory must not be null.");
    }

    @Override
    public Transaction startTransaction() {
        throw new UnsupportedOperationException("Only runs the units of work of the workflow processor.");
    }

    @Override
    public void attachToProcessingLifecycle(ProcessingLifecycle processingLifecycle) {
        processingLifecycle.runOnPreInvocation(context -> {
            var transaction = new LazyTransaction();
            context.putResource(JpaTransactionalExecutorProvider.SUPPLIER_KEY, transaction);
            context.runOnCommit(committed -> transaction.commit());
            context.onError((failed, phase, error) -> transaction.rollback());
        });
    }

    /**
     * The transaction of one unit of work, begun when it is first used. Guarded by a lock rather than a monitor, as
     * beginning a transaction may block on the connection pool (no blocking under a monitor on a virtual thread).
     */
    private final class LazyTransaction implements Supplier<EntityManagerExecutor> {

        private final ReentrantLock lock = new ReentrantLock();
        private @Nullable EntityManager entityManager;
        private @Nullable EntityManagerExecutor executor;
        private boolean concluded;

        @Override
        public EntityManagerExecutor get() {
            lock.lock();
            try {
                if (concluded) {
                    throw new IllegalStateException("The transaction of this unit of work already ended.");
                }
                if (executor == null) {
                    var opened = entityManagerFactory.createEntityManager();
                    opened.getTransaction().begin();
                    entityManager = opened;
                    executor = new EntityManagerExecutor(() -> opened);
                }
                return executor;
            } finally {
                lock.unlock();
            }
        }

        void commit() {
            conclude(true);
        }

        void rollback() {
            conclude(false);
        }

        private void conclude(boolean commit) {
            lock.lock();
            try {
                if (concluded) {
                    return;
                }
                concluded = true;
                var opened = entityManager;
                if (opened == null) {
                    return;
                }
                try {
                    var transaction = opened.getTransaction();
                    if (commit) {
                        transaction.commit();
                    } else if (transaction.isActive()) {
                        transaction.rollback();
                    }
                } finally {
                    opened.close();
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
