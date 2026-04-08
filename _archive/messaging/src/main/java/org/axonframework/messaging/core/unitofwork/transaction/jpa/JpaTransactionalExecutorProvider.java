/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.core.unitofwork.transaction.jpa;

import org.jspecify.annotations.Nullable;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.jpa.EntityManagerExecutor;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A {@link TransactionalExecutorProvider} implementation for JPA {@link EntityManager EntityManagers} which
 * provides a {@link TransactionalExecutor}.
 * <p>
 * When a processing context is supplied, supplies the {@link TransactionalExecutor} it must contain.
 * If no processing context is supplied, creates an executor that executes the supplied functions
 * in their own transaction.
 *
 * @author John Hendrikx
 * @since 5.0.2
 */
@Internal
public class JpaTransactionalExecutorProvider implements TransactionalExecutorProvider<EntityManager> {

    /**
     * The resource key for the {@link EntityManagerExecutor} supplier.
     */
    public static final ResourceKey<Supplier<EntityManagerExecutor>> SUPPLIER_KEY = ResourceKey.withLabel(EntityManagerExecutor.class.getSimpleName());

    private final EntityManagerFactory entityManagerFactory;

    /**
     * Constructs a new instance.
     *
     * @param entityManagerFactory A factory constructing an {@link EntityManager} used when no processing context is supplied.
     */
    public JpaTransactionalExecutorProvider(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = Objects.requireNonNull(entityManagerFactory, "entityManagerFactory");
    }

    @Override
    public TransactionalExecutor<EntityManager> getTransactionalExecutor(@Nullable ProcessingContext processingContext) {
        if (processingContext != null) {
            Supplier<EntityManagerExecutor> executorSupplier = processingContext.getResource(SUPPLIER_KEY);

            if (executorSupplier == null) {
                throw new IllegalStateException("An entity manager executor must be present in the processing context.");
            }

            return executorSupplier.get();
        }

        return new TransactionalExecutor<>() {
            @Override
            public <R> CompletableFuture<R> apply(ThrowingFunction<EntityManager, R, Exception> function) {
                // This uses the entity manager factory directly, so this always runs in its own transaction:
                EntityManager entityManager = entityManagerFactory.createEntityManager();
                EntityTransaction tx = entityManager.getTransaction();

                try {
                    tx.begin();

                    R result = function.apply(entityManager);

                    tx.commit();

                    return CompletableFuture.completedFuture(result);
                }
                catch (Exception e) {
                    if (tx.isActive()) {
                        tx.rollback();
                    }

                    return CompletableFuture.failedFuture(e);
                }
                finally {
                    entityManager.close();
                }
            }
        };
    }
}
