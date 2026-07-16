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
package io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.jpa;

import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.Converter;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.TenantTokenStoreFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A {@link TenantTokenStoreFactory} that creates {@link JpaTokenStore} instances for each tenant.
 * <p>
 * This factory uses a provider function to obtain tenant-specific {@link EntityManagerFactory} instances.
 * Each tenant gets its own {@link JpaTokenStore} that persists tokens to that tenant's database,
 * ensuring transactional consistency and data isolation.
 * <p>
 * Token stores are cached per tenant to avoid creating multiple instances for the same tenant.
 * <p>
 * Example usage:
 * <pre>{@code
 * JpaTenantTokenStoreFactory tokenStoreFactory = new JpaTenantTokenStoreFactory(
 *     tenant -> getEntityManagerFactoryForTenant(tenant),
 *     converter
 * );
 *
 * // Register with the component registry
 * configurer.componentRegistry(cr ->
 *     cr.registerComponent(TenantTokenStoreFactory.class, config -> tokenStoreFactory)
 * );
 * }</pre>
 *
 * @author Jan Galinski
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see TenantTokenStoreFactory
 * @see JpaTokenStore
 */
public class JpaTenantTokenStoreFactory implements TenantTokenStoreFactory {

    private final Function<TenantDescriptor, EntityManagerFactory> emfProvider;
    private final Converter converter;
    private final JpaTokenStoreConfiguration configuration;
    private final Map<TenantDescriptor, TokenStore> tokenStores = new ConcurrentHashMap<>();

    /**
     * Creates a new {@link JpaTenantTokenStoreFactory} with the given dependencies.
     *
     * @param emfProvider   function that provides an {@link EntityManagerFactory} for each tenant
     * @param converter     the converter for serializing/deserializing tokens
     * @param configuration the JPA token store configuration
     */
    public JpaTenantTokenStoreFactory(
            Function<TenantDescriptor, EntityManagerFactory> emfProvider,
            Converter converter,
            JpaTokenStoreConfiguration configuration
    ) {
        this.emfProvider = Objects.requireNonNull(emfProvider,
                "EntityManagerFactory provider must not be null");
        this.converter = Objects.requireNonNull(converter, "Converter must not be null");
        this.configuration = Objects.requireNonNull(configuration, "JpaTokenStoreConfiguration must not be null");
    }

    /**
     * Creates a new {@link JpaTenantTokenStoreFactory} with default configuration.
     *
     * @param emfProvider function that provides an {@link EntityManagerFactory} for each tenant
     * @param converter   the converter for serializing/deserializing tokens
     */
    public JpaTenantTokenStoreFactory(
            Function<TenantDescriptor, EntityManagerFactory> emfProvider,
            Converter converter
    ) {
        this(emfProvider, converter, JpaTokenStoreConfiguration.DEFAULT);
    }

    @Override
    public TokenStore apply(TenantDescriptor tenant) {
        return tokenStores.computeIfAbsent(tenant, this::createTokenStore);
    }

    private TokenStore createTokenStore(TenantDescriptor tenant) {
        EntityManagerFactory emf = emfProvider.apply(tenant);
        return new JpaTokenStore(new JpaTransactionalExecutorProvider(emf), converter, configuration);
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
