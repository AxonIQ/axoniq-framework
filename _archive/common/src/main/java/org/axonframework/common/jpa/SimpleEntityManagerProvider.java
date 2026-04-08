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
import org.axonframework.common.Assert;

/**
 * Simple implementation of the EntityManagerProvider that returns the EntityManager instance provided at construction
 * time.
 *
 * @author Allard Buijze
 * @since 1.3
 */
public class SimpleEntityManagerProvider implements EntityManagerProvider {

    private final EntityManager entityManager;

    /**
     * Initializes an instance that always returns the given {@code entityManager}. This class can be used for
     * testing, or when using a ContainerManaged EntityManager.
     *
     * @param entityManager the EntityManager to return on {@link #getEntityManager()}
     */
    public SimpleEntityManagerProvider(EntityManager entityManager) {
        Assert.notNull(entityManager, () -> "entityManager should not be null");
        this.entityManager = entityManager;
    }

    @Override
    public EntityManager getEntityManager() {
        return entityManager;
    }
}
