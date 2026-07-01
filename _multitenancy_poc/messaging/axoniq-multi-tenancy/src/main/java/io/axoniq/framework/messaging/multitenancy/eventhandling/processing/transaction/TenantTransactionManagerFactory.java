/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.framework.messaging.multitenancy.eventhandling.processing.transaction;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;

import java.util.function.Function;

/**
 * Factory for creating tenant-specific {@link TransactionManager} instances.
 * <p>
 * The multi-tenant pooled streaming processor uses this factory to bind a tenant-local transaction manager to the
 * processor's {@link org.axonframework.messaging.core.unitofwork.UnitOfWork} so JDBC and JPA token stores can expose
 * their transactional executor to the processing context.
 *
 * @author Jan Galinski
 * @since 5.3.0
 * @see TransactionManager
 */
@FunctionalInterface
public interface TenantTransactionManagerFactory extends Function<TenantDescriptor, TransactionManager> {

    /**
     * Creates or retrieves a {@link TransactionManager} for the specified tenant.
     *
     * @param tenant the tenant descriptor identifying the tenant
     * @return a tenant-specific {@link TransactionManager}
     */
    @Override
    TransactionManager apply(TenantDescriptor tenant);
}
