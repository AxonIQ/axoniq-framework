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

package io.axoniq.platform.framework.eventprocessor

import io.axoniq.platform.framework.api.metrics.ChildHandlerMetric
import io.axoniq.platform.framework.api.metrics.HandlerStatisticsMetricIdentifier
import io.axoniq.platform.framework.api.metrics.HandlerType
import io.axoniq.platform.framework.messaging.HandlerMeasurement
import io.axoniq.platform.framework.messaging.HandlerMeasurement.Companion.RESOURCE_KEY
import io.axoniq.platform.framework.messaging.HandlerMetricsRegistry
import io.axoniq.platform.framework.messaging.toInformation
import io.github.oshai.kotlinlogging.KotlinLogging
import org.axonframework.messaging.core.Message
import org.axonframework.messaging.core.MessageStream
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.core.unitofwork.ProcessingContext
import org.axonframework.messaging.eventhandling.EventHandler
import org.axonframework.messaging.eventhandling.EventHandlerRegistry
import org.axonframework.messaging.eventhandling.EventHandlingComponent
import org.axonframework.messaging.eventhandling.EventMessage
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment
import java.time.Instant
import java.time.temporal.ChronoUnit

class AxoniqPlatformEventHandlingComponent(
        private val delegate: EventHandlingComponent,
        private val processorName: String,
        private val handlerMetricsRegistry: HandlerMetricsRegistry,
        private val processorMetricRegistry: ProcessorMetricsRegistry
) : EventHandlingComponent {
    private val logger = KotlinLogging.logger { }
    override fun handle(event: EventMessage, context: ProcessingContext): MessageStream.Empty<Message?> {
        if (context.containsResource(RESOURCE_KEY)) {
            val startTime = System.nanoTime()
            // This is a subscribing event handler called from another event handler in the same component.
            val originalMeasurements = context.getResource(RESOURCE_KEY)
            val eventResult = delegate.handle(event, context)
            eventResult.onComplete {
                originalMeasurements.registerMetricValue(
                        ChildHandlerMetric(
                                handler = HandlerStatisticsMetricIdentifier(
                                        type = HandlerType.EventProcessor,
                                        component = processorName,
                                        message = event.toInformation()
                                ),
                        ), System.nanoTime() - startTime)
            }
            return eventResult
        }
        val measurement = HandlerMeasurement(event, HandlerType.EventProcessor, processorName)
        val contextWithMeasurements = context.withResource(RESOURCE_KEY, measurement)

        // Report ingest latency
        logger.debug { "Registering ingest for event [${event.type()}][${event.identifier()}] in processor [$processorName]" }
        val ingestTimestamp = Instant.now()
        val segment = contextWithMeasurements.getResource(Segment.RESOURCE_KEY)
        processorMetricRegistry.registerIngested(processorName, segment.segmentId, ChronoUnit.NANOS.between(event.timestamp(), ingestTimestamp))
        contextWithMeasurements.runOnAfterCommit {
            logger.debug { "Processed event [${event.type()}][${event.identifier()}] successfully in processor [$processorName]" }
            processorMetricRegistry.registerCommitted(processorName, segment.segmentId, ChronoUnit.NANOS.between(ingestTimestamp, Instant.now()))
            measurement.complete(true)
            handlerMetricsRegistry.registerMeasurement(measurement)
        }
        contextWithMeasurements.onError { _, _, _ ->
            logger.debug { "Error in processing event [${event.type()}][${event.identifier()}] in processor [$processorName]" }
            measurement.complete(false)
            handlerMetricsRegistry.registerMeasurement(measurement)
        }

        processorMetricRegistry.registerActiveMessage(contextWithMeasurements, processorName, segment.segmentId, event.timestamp())
        return delegate.handle(event, contextWithMeasurements)
    }

    override fun supportedEvents(): Set<QualifiedName> {
        return delegate.supportedEvents()
    }

    override fun sequenceIdentifierFor(event: EventMessage, context: ProcessingContext): Any {
        return delegate.sequenceIdentifierFor(event, context)
    }

    override fun subscribe(name: QualifiedName, eventHandler: EventHandler): EventHandlerRegistry? {
        delegate.subscribe(name, eventHandler)
        return this
    }
}