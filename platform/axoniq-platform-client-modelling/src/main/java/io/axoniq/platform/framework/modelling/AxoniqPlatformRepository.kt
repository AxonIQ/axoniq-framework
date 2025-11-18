/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
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

package io.axoniq.platform.framework.modelling

import io.axoniq.platform.framework.api.metrics.LoadModelMetric
import io.axoniq.platform.framework.messaging.HandlerMeasurement
import org.axonframework.common.infra.ComponentDescriptor
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.axonframework.modelling.repository.ManagedEntity
import org.axonframework.modelling.repository.Repository
import java.util.concurrent.CompletableFuture

class AxoniqPlatformRepository<ID : Any, E : Any>(
        private val delegate: Repository<ID, E>,
) : Repository.LifecycleManagement<ID, E> {
    override fun attach(entity: ManagedEntity<ID, E?>, processingContext: ProcessingContext): ManagedEntity<ID, E> {
        val lfcm = delegate as Repository.LifecycleManagement<ID, E>
        return lfcm.attach(entity, processingContext)
    }

    override fun entityType(): Class<E> {
        return delegate.entityType()
    }

    override fun idType(): Class<ID> {
        return delegate.idType()
    }

    override fun load(identifier: ID, processingContext: ProcessingContext): CompletableFuture<ManagedEntity<ID, E>> {
        val startTime = System.nanoTime()
        return delegate.load(identifier, processingContext).whenComplete { _, _ ->
            val duration = System.nanoTime() - startTime
            HandlerMeasurement.onContext(processingContext) {
                it.registerMetricValue(LoadModelMetric(identifier = "load:${entityType().simpleName}"), duration)
            }
        }
    }

    override fun loadOrCreate(identifier: ID, processingContext: ProcessingContext): CompletableFuture<ManagedEntity<ID, E>> {
        val startTime = System.nanoTime()
        return delegate.loadOrCreate(identifier, processingContext).whenComplete { _, _ ->
            val duration = System.nanoTime() - startTime
            HandlerMeasurement.onContext(processingContext) {
                it.registerMetricValue(LoadModelMetric(identifier = "load:${entityType().simpleName}"), duration)
            }
        }
    }

    override fun persist(identifier: ID, entity: E, processingContext: ProcessingContext): ManagedEntity<ID, E> {
        return delegate.persist(identifier, entity, processingContext)
    }

    override fun describeTo(descriptor: ComponentDescriptor) {
        descriptor.describeWrapperOf(delegate)
    }
}