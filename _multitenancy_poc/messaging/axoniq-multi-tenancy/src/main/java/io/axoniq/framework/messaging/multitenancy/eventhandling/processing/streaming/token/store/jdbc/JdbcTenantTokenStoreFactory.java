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

import org.axonframework.conversion.Converter;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.TenantTokenStoreFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * A {@link TenantTokenStoreFactory} that creates {@link JdbcTokenStore} instances for each tenant.
 * <p>
 * This factory uses a {@link TenantConnectionProviderFactory} to obtain tenant-specific
 * JDBC connections. Each tenant gets its own {@link JdbcTokenStore} that persists tokens
 * to that tenant's database, ensuring transactional consistency and data isolation.
 * <p>
 * Token stores are cached per tenant to avoid creating multiple instances for the same tenant.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a connection provider factory that returns tenant-specific data sources
 * TenantConnectionProviderFactory connectionFactory = tenant -> getDataSourceForTenant(tenant);
 *
 * // Register the factory with configuration
 * configurer.registerComponent(TenantTokenStoreFactory.class, cfg ->
 *     new JdbcTenantTokenStoreFactory(
 *         connectionFactory,
 *         cfg.getComponent(Converter.class),
 *         JdbcTokenStoreConfiguration.DEFAULT
 *     )
 * );
 * }</pre>
 *
 * @author Jan Galinski
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see TenantTokenStoreFactory
 * @see JdbcTokenStore
 * @see TenantConnectionProviderFactory
 */
public class JdbcTenantTokenStoreFactory implements TenantTokenStoreFactory {

    private final TenantConnectionProviderFactory connectionProviderFactory;
    private final Converter converter;
    private final JdbcTokenStoreConfiguration configuration;
    private final Map<TenantDescriptor, TokenStore> tokenStores = new ConcurrentHashMap<>();

    /**
     * Creates a new {@link JdbcTenantTokenStoreFactory} with the given dependencies.
     *
     * @param connectionProviderFactory the factory for obtaining tenant-specific ConnectionProviders
     * @param converter                 the converter for serializing/deserializing tokens
     * @param configuration             the JDBC token store configuration
     */
    public JdbcTenantTokenStoreFactory(
            TenantConnectionProviderFactory connectionProviderFactory,
            Converter converter,
            JdbcTokenStoreConfiguration configuration
    ) {
        this.connectionProviderFactory = Objects.requireNonNull(connectionProviderFactory,
                "TenantConnectionProviderFactory must not be null");
        this.converter = Objects.requireNonNull(converter, "Converter must not be null");
        this.configuration = Objects.requireNonNull(configuration, "JdbcTokenStoreConfiguration must not be null");
    }

    /**
     * Creates a new {@link JdbcTenantTokenStoreFactory} with default configuration.
     *
     * @param connectionProviderFactory the factory for obtaining tenant-specific ConnectionProviders
     * @param converter                 the converter for serializing/deserializing tokens
     */
    public JdbcTenantTokenStoreFactory(
            TenantConnectionProviderFactory connectionProviderFactory,
            Converter converter
    ) {
        this(connectionProviderFactory, converter, JdbcTokenStoreConfiguration.DEFAULT);
    }

    @Override
    public TokenStore apply(TenantDescriptor tenant) {
        return tokenStores.computeIfAbsent(tenant, this::createTokenStore);
    }

    private TokenStore createTokenStore(TenantDescriptor tenant) {
        DataSource tenantDataSource = connectionProviderFactory.apply(tenant);
        return new JdbcTokenStore(new JdbcTransactionalExecutorProvider(tenantDataSource), converter, configuration);
    }

    /**
     * Returns the number of token stores currently cached.
     * Primarily for testing purposes.
     *
     * @return the number of cached token stores
     */
    int tokenStoreCount() {
        return tokenStores.size();
    }
}
