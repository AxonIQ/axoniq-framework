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

import org.jspecify.annotations.NonNull;

/**
 * A non-final {@link TransactionManager} implementation, so that it can be spied upon through Mockito.
 */
public class NoOpTransactionManager implements TransactionManager {

    @Override
    public @NonNull Transaction startTransaction() {
        return new Transaction() {
            @Override
            public void commit() {
                // No-op
            }

            @Override
            public void rollback() {
                // No-op
            }
        };
    }
}
