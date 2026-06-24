/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.jdbc;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;

import java.util.function.Function;
import javax.sql.DataSource;

/**
 * Factory for creating tenant-specific {@link DataSource} instances.
 * <p>
 * This factory is used by {@link JdbcTenantTokenStoreFactory} to create
 * per-tenant JDBC data sources for token storage.
 * <p>
 * Implementations should ensure that each tenant gets a {@link DataSource} for the appropriate
 * database. For example, when using a database-per-tenant architecture, each tenant's
 * {@link DataSource} should point to that tenant's specific database.
 * <p>
 * Example implementation using DataSource per tenant:
 * <pre>{@code
 * TenantConnectionProviderFactory factory = tenant -> getDataSourceForTenant(tenant);
 * }</pre>
 *
 * @author Jan Galinski
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see JdbcTenantTokenStoreFactory
 */
@FunctionalInterface
public interface TenantConnectionProviderFactory extends Function<TenantDescriptor, DataSource> {

    /**
     * Creates or retrieves a {@link DataSource} for the specified tenant.
     *
     * @param tenant the tenant descriptor identifying the tenant
     * @return a {@link DataSource} that provides connections to the tenant's database
     */
    @Override
    DataSource apply(TenantDescriptor tenant);
}
