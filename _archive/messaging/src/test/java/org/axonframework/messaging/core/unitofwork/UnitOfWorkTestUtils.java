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

package org.axonframework.messaging.core.unitofwork;

import java.util.UUID;


import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.jspecify.annotations.NonNull;

/**
 * Test utilities when dealing with {@link UnitOfWork}.
 *
 * @author Mateusz Nowak
 */
public final class UnitOfWorkTestUtils {

    public static final SimpleUnitOfWorkFactory SIMPLE_FACTORY = new SimpleUnitOfWorkFactory(
            EmptyApplicationContext.INSTANCE
    );

    /**
     * Creates a new {@link UnitOfWork} with the given identifier.
     * <p>
     * Please note this instance will be created using the {@link SimpleUnitOfWorkFactory} with an
     * {@link EmptyApplicationContext}, so you will not be able to get any components from the
     * {@link ProcessingContext#component} method - it will always throw a
     * {@link ComponentNotFoundException}.
     *
     * @return A new {@link UnitOfWork} with the random identifier.
     */
    public static @NonNull UnitOfWork aUnitOfWork() {
        return SIMPLE_FACTORY.create(UUID.randomUUID().toString());
    }

    /**
     * Creates a new {@link UnitOfWork} with the given identifier.
     * <p>
     * Please note this instance will be created using the {@link SimpleUnitOfWorkFactory} with an
     * {@link EmptyApplicationContext}, so you will not be able to get any components from the
     * {@link ProcessingContext#component} method - it will always throw a
     * {@link ComponentNotFoundException}.
     *
     * @param identifier The identifier for the {@link UnitOfWork}.
     * @return A new {@link UnitOfWork} with the given identifier.
     */
    public static @NonNull UnitOfWork aUnitOfWork(@NonNull String identifier) {
        return SIMPLE_FACTORY.create(identifier);
    }

    /**
     * Creates a {@link TransactionalUnitOfWorkFactory} configured with the given {@link TransactionManager}. The
     * resulting factory creates {@link UnitOfWork} instances bound to transactions managed by the specified
     * {@link TransactionManager}.
     * <p>
     * Please note this will delegate to the {@link SimpleUnitOfWorkFactory} with an {@link EmptyApplicationContext}, so
     * you will not be able to get any components from the {@link ProcessingContext#component} method - it will always
     * throw a {@link ComponentNotFoundException}.
     *
     * @param transactionManager The transaction manager used to manage transactions for the units of work.
     * @return A new instance of {@link TransactionalUnitOfWorkFactory} using the provided transaction manager.
     */
    public static TransactionalUnitOfWorkFactory transactionalUnitOfWorkFactory(TransactionManager transactionManager) {
        return new TransactionalUnitOfWorkFactory(transactionManager, SIMPLE_FACTORY);
    }

    private UnitOfWorkTestUtils() {
        // Utility class
    }
}
