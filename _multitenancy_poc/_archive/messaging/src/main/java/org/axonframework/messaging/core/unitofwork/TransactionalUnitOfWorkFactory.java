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

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

import java.util.Objects;
import java.util.function.Function;

/**
 * Factory for creating {@link UnitOfWork} instances that are bound to a transaction.
 * <p>
 * This factory creates units of work that automatically start a transaction before invocation, commit the transaction
 * on successful completion, and roll back the transaction when an error occurs.
 * <p>
 * The transaction is managed by the configured {@link TransactionManager} and is stored as a resource in the unit of
 * work's {@link Context}.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class TransactionalUnitOfWorkFactory implements UnitOfWorkFactory {

    private final TransactionManager transactionManager;
    private final UnitOfWorkFactory delegate;

    /**
     * Initializes a factory with the given {@code transactionManager} and a delegate {@link UnitOfWorkFactory}. The
     * unit of work's lifecycle will be bound to transaction managed by the provided {@code transactionManager}.
     *
     * @param transactionManager the transaction manager used to create and manage transactions for the units of work
     * @param delegate           the delegate factory used to create units of work
     */
    public TransactionalUnitOfWorkFactory(
            TransactionManager transactionManager,
            UnitOfWorkFactory delegate
    ) {
        Objects.requireNonNull(transactionManager, "Transaction Manager cannot be null");
        Objects.requireNonNull(delegate, "Delegate UnitOfWorkFactory cannot be null");
        this.transactionManager = transactionManager;
        this.delegate = delegate;
    }

    /**
     * Creates a new {@link UnitOfWork} that is bound to a transaction.
     * <p>
     * The created unit of work will:
     * <ul>
     *     <li>Start a new transaction before invocation using the configured {@link TransactionManager}.</li>
     *     <li>Commit the transaction when the unit of work is committed.</li>
     *     <li>Roll back the transaction when an error occurs during any phase of the unit of work.</li>
     * </ul>
     * The transaction is stored as a resource in the unit of work's context using a resource key with label "transaction".
     *
     * @return a new transactional unit of work
     */
    @Override
    public UnitOfWork create(
            String identifier,
            Function<UnitOfWorkConfiguration, UnitOfWorkConfiguration> customization
    ) {
        if (transactionManager.requiresSameThreadInvocations()) {
            customization = customization.andThen(UnitOfWorkConfiguration::forcedSameThreadInvocation);
        }
        var unitOfWork = delegate.create(identifier, customization);

        transactionManager.attachToProcessingLifecycle(unitOfWork);

        return unitOfWork;
    }
}
