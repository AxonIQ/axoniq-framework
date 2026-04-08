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

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Provider of {@link TransactionalExecutor TransactionalExecutors}.
 *
 * @param <T> The type of resource the {@link TransactionalExecutor} works with.
 * @author John Hendrikx
 * @since 5.0.2
 */
@Internal
public interface TransactionalExecutorProvider<T> {

    /**
     * Provides a {@link TransactionalExecutor}, using the optional processing context.
     *
     * @param processingContext A {@link ProcessingContext}, can be {@code null}.
     * @return A {@link TransactionalExecutor}, never {@code null}.
     */
    TransactionalExecutor<T> getTransactionalExecutor(@Nullable ProcessingContext processingContext);
}
