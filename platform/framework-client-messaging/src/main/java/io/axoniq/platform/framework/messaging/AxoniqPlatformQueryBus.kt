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

package io.axoniq.platform.framework.messaging

import io.axoniq.platform.framework.api.metrics.HandlerType
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.common.infra.ComponentDescriptor
import org.axonframework.messaging.core.MessageStream
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.axonframework.messaging.queryhandling.*
import org.axonframework.messaging.queryhandling.distributed.DistributedQueryBusConfigurationEnhancer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Predicate
import java.util.function.Supplier

class AxoniqPlatformQueryBus(
        private val delegate: QueryBus,
        private val metricsRegistry: HandlerMetricsRegistry,
) : QueryBus {
    private val logger = KotlinLogging.logger { }

    companion object {
        const val DECORATION_ORDER = DistributedQueryBusConfigurationEnhancer.DISTRIBUTED_QUERY_BUS_ORDER - 1
    }

    override fun describeTo(descriptor: ComponentDescriptor) {
        descriptor.describeWrapperOf(delegate)
    }

    override fun subscribe(name: QualifiedName, queryHandler: QueryHandler): QueryBus? {
        logger.debug { "Decorating query handler subscription for [$name] and handler [$queryHandler]" }
        delegate.subscribe(name) { command, context ->
            logger.debug { "Handling query [${command.type()}] with handler [$queryHandler]" }
            val measurement = HandlerMeasurement(message = command, handlerType = HandlerType.Message)
            context.putResource(HandlerMeasurement.RESOURCE_KEY, measurement)
            val ranAfterCommit = AtomicBoolean()
            val stream = queryHandler.handle(command, context)
            context.runOnAfterCommit {
                ranAfterCommit.set(true)
                logger.debug { "Registering success for query [${command.type()}] with handler [$queryHandler]" }
                measurement.complete(true)
                metricsRegistry.registerMeasurement(measurement)
            }
            context.onError { _, _, _ ->
                if (!ranAfterCommit.get()) {
                    logger.debug { "Registering failure for query [${command.type()}] with handler [$queryHandler]" }
                    measurement.complete(false)
                    metricsRegistry.registerMeasurement(measurement)
                }
            }
            stream
        }
        return this
    }

    override fun query(query: QueryMessage, context: ProcessingContext?): MessageStream<QueryResponseMessage?> {
        val measurementContainer = context?.getResource(HandlerMeasurement.RESOURCE_KEY)
        if (measurementContainer == null) {
            metricsRegistry.registerMessageDispatchedWithoutHandling(query.toInformation())
        } else {
            measurementContainer.reportMessageDispatched(query.toInformation())
        }

        return delegate.query(query, context)
    }

    override fun subscriptionQuery(query: QueryMessage, context: ProcessingContext?, updateBufferSize: Int): MessageStream<QueryResponseMessage?> {
        val measurementContainer = context?.getResource(HandlerMeasurement.RESOURCE_KEY)
        if (measurementContainer == null) {
            metricsRegistry.registerMessageDispatchedWithoutHandling(query.toInformation())
        } else {
            measurementContainer.reportMessageDispatched(query.toInformation())
        }

        return delegate.subscriptionQuery(query, context, updateBufferSize)
    }

    override fun subscribeToUpdates(query: QueryMessage, updateBufferSize: Int): MessageStream<SubscriptionQueryUpdateMessage?> {
        return delegate.subscribeToUpdates(query, updateBufferSize)
    }

    override fun emitUpdate(filter: Predicate<QueryMessage>, updateSupplier: Supplier<SubscriptionQueryUpdateMessage>, context: ProcessingContext?): CompletableFuture<Void> {
        return delegate.emitUpdate(filter, {
            val update = updateSupplier.get()
            val measurementContainer = context?.getResource(HandlerMeasurement.RESOURCE_KEY)
            if (measurementContainer == null) {
                metricsRegistry.registerMessageDispatchedWithoutHandling(update.toInformation())
            } else {
                measurementContainer.reportMessageDispatched(update.toInformation())
            }
            update

        }, context)
    }

    override fun completeSubscriptions(filter: Predicate<QueryMessage?>, context: ProcessingContext?): CompletableFuture<Void?> {
        return delegate.completeSubscriptions(filter, context)
    }

    override fun completeSubscriptionsExceptionally(filter: Predicate<QueryMessage?>, cause: Throwable, context: ProcessingContext?): CompletableFuture<Void?> {
        return delegate.completeSubscriptionsExceptionally(filter, cause, context)
    }
}