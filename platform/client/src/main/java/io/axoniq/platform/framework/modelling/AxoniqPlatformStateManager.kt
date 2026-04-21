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

package io.axoniq.platform.framework.modelling

import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.axonframework.modelling.StateManager
import org.axonframework.modelling.repository.ManagedEntity
import org.axonframework.modelling.repository.Repository
import java.util.concurrent.CompletableFuture

class AxoniqPlatformStateManager(
        private val delegate: StateManager
): StateManager {
    override fun <ID: Any, T: Any> register(repository: Repository<ID, T>): StateManager {
        delegate.register<ID, T>(AxoniqPlatformRepository(repository))
        return this
    }

    override fun <ID : Any, T : Any> loadManagedEntity(type: Class<T>, id: ID, context: ProcessingContext): CompletableFuture<ManagedEntity<ID, T>> {
        return delegate.loadManagedEntity(type, id, context)
    }

    override fun registeredEntities(): Set<Class<*>> {
        return delegate.registeredEntities()
    }

    override fun registeredIdsFor(entityType: Class<*>): Set<Class<*>> {
        return delegate.registeredIdsFor(entityType)
    }

    override fun <ID : Any, T : Any> repository(entityType: Class<T>, idType: Class<ID>): Repository<ID, T> {
        return delegate.repository(entityType, idType)
    }

}