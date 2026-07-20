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
package io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;

import java.util.function.Function;

/**
 * Factory for creating tenant-specific {@link TokenStore} instances.
 * <p>
 * Used by the multi-tenancy infrastructure to create per-tenant token stores
 * for tracking event processing progress. Each tenant gets an isolated token
 * store to prevent token conflicts between tenants.
 *
 * @author Jan Galinski
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see TokenStore
 */
@FunctionalInterface
public interface TenantTokenStoreFactory extends Function<TenantDescriptor, TokenStore> {

    /**
     * Creates or retrieves a {@link TokenStore} for the specified tenant.
     *
     * @param tenant the tenant descriptor identifying the tenant
     * @return a {@link TokenStore} instance for the specified tenant
     */
    @Override
    TokenStore apply(TenantDescriptor tenant);
}
