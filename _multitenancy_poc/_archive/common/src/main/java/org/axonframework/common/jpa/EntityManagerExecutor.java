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

package org.axonframework.common.jpa;

import jakarta.persistence.EntityManager;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.TransactionalExecutor;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link TransactionalExecutor} implementation for {@link EntityManager EntityManagers}.
 *
 * @author John Hendrikx
 * @since 5.0.2
 */
@Internal
public class EntityManagerExecutor implements TransactionalExecutor<EntityManager> {
    private final EntityManagerProvider provider;

    /**
     * Creates a new instance.
     *
     * @param provider An {@link EntityManagerProvider}, cannot be {@code null}.
     * @throws NullPointerException If any argument is {@code null}.
     */
    public EntityManagerExecutor(EntityManagerProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider");
    }

    @Override
    public <R> CompletableFuture<R> apply(ThrowingFunction<EntityManager, R, Exception> function) {
        try {
            return CompletableFuture.completedFuture(function.apply(provider.getEntityManager()));
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
