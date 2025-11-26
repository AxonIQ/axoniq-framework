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

package io.axoniq.platform.framework.eventsourcing

import io.axoniq.platform.framework.api.metrics.PreconfiguredMetric
import io.axoniq.platform.framework.messaging.HandlerMeasurement
import io.axoniq.platform.framework.messaging.HandlerMeasurement.Companion.RESOURCE_KEY
import io.axoniq.platform.framework.messaging.HandlerMetricsRegistry
import io.axoniq.platform.framework.messaging.toInformation
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.common.infra.ComponentDescriptor
import org.axonframework.eventsourcing.eventstore.AppendCondition
import org.axonframework.eventsourcing.eventstore.EventStorageEngine
import org.axonframework.eventsourcing.eventstore.SourcingCondition
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage
import org.axonframework.messaging.core.MessageStream
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.axonframework.messaging.eventhandling.EventMessage
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken
import org.axonframework.messaging.eventstreaming.StreamingCondition
import java.time.Instant
import java.util.concurrent.CompletableFuture

class AxoniqPlatformEventStorageEngine(
        private val delegate: EventStorageEngine,
        private val registry: HandlerMetricsRegistry,
) : EventStorageEngine {
    private val logger = KotlinLogging.logger { }

    override fun appendEvents(condition: AppendCondition, context: ProcessingContext?, events: List<TaggedEventMessage<*>>): CompletableFuture<EventStorageEngine.AppendTransaction<*>> {
        // First, report dispatches
        val container = context?.getResource(RESOURCE_KEY)
        if (container == null) {
            events.forEach { tm ->
                val event: EventMessage = tm.event()
                registry.registerMessageDispatchedWithoutHandling(event.toInformation())
            }
        } else {
            events.forEach { tm ->
                val event: EventMessage = tm.event()
                container.reportMessageDispatched(event.toInformation())
            }
        }
        if (context == null) {
            return delegate.appendEvents(condition, context, events)
        }
        // Second, measure commit time if measurement is ongoing
        val startTime = System.nanoTime()
        return delegate.appendEvents(condition, context, events)
                .whenComplete { _, _ ->
                    val endTime = System.nanoTime()
                    HandlerMeasurement.onContext(context) {
                        it.registerMetricValue(PreconfiguredMetric.EVENT_COMMIT_TIME, endTime - startTime)
                    }
                }
    }

    override fun appendEvents(condition: AppendCondition, context: ProcessingContext?, vararg events: TaggedEventMessage<*>): CompletableFuture<EventStorageEngine.AppendTransaction<*>> {
        return appendEvents(condition, context, events.toList())
    }

    override fun source(condition: SourcingCondition, context: ProcessingContext?): MessageStream<EventMessage?> {
        return delegate.source(condition, context)
    }

    override fun stream(condition: StreamingCondition, context: ProcessingContext?): MessageStream<EventMessage?> {
        return delegate.stream(condition, context)
    }

    override fun firstToken(context: ProcessingContext?): CompletableFuture<TrackingToken?> {
        return delegate.firstToken(context)
    }

    override fun latestToken(context: ProcessingContext?): CompletableFuture<TrackingToken?> {
        return delegate.latestToken(context)
    }

    override fun tokenAt(at: Instant, context: ProcessingContext?): CompletableFuture<TrackingToken?> {
        return delegate.tokenAt(at, context)
    }

    override fun describeTo(descriptor: ComponentDescriptor) {
        descriptor.describeWrapperOf(delegate)
    }
}