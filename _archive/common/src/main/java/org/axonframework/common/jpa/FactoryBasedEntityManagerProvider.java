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
import jakarta.persistence.EntityManagerFactory;

import java.util.Objects;

/**
 * An implementation of the {@link EntityManagerProvider} that returns a new {@link EntityManager} instance
 * each time using the {@link EntityManagerFactory}.
 *
 * @author John Hendrikx
 * @since 5.0.2
 */
public class FactoryBasedEntityManagerProvider implements EntityManagerProvider {
    private final EntityManagerFactory entityManagerFactory;

    /**
     * Constructs a new instance.
     *
     * @param entityManagerFactory The {@link EntityManagerFactory} to use, cannot be {@code null}.
     */
    public FactoryBasedEntityManagerProvider(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = Objects.requireNonNull(entityManagerFactory, "entityManagerFactory");
    }

    @Override
    public EntityManager getEntityManager() {
        return entityManagerFactory.createEntityManager();
    }
}
