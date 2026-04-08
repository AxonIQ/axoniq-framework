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

package org.axonframework.messaging.core.unitofwork.transaction;

/**
 * A {@link TransactionManager} implementation that does nothing.
 * <p>
 * It's a placeholder implementation for the cases where no special transaction management is required.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public enum NoTransactionManager implements TransactionManager {

    /**
     * Singleton instance of the {@link TransactionManager}.
     */
    INSTANCE;

    /**
     * Returns the singleton instance of this {@link TransactionManager}.
     *
     * @return the singleton instance of this {@link TransactionManager}
     */
    public static TransactionManager instance() {
        return INSTANCE;
    }


    @Override
    public Transaction startTransaction() {
        return TRANSACTION;
    }

    private static final Transaction TRANSACTION = new Transaction() {
        @Override
        public void commit() {
            //no op
        }

        @Override
        public void rollback() {
            //no op
        }
    };
}
