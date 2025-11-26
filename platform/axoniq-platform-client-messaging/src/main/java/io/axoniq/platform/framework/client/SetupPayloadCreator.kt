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

package io.axoniq.platform.framework.client

import io.axoniq.platform.framework.api.AxonServerEventStoreMessageSourceInformation
import io.axoniq.platform.framework.api.CommandBusInformation
import io.axoniq.platform.framework.api.CommonProcessorInformation
import io.axoniq.platform.framework.api.EventProcessorInformation
import io.axoniq.platform.framework.api.EventStoreInformation
import io.axoniq.platform.framework.api.InterceptorInformation
import io.axoniq.platform.framework.api.MessageSourceInformation
import io.axoniq.platform.framework.api.ModuleVersion
import io.axoniq.platform.framework.api.PooledStreamingEventProcessorInformation
import io.axoniq.platform.framework.api.ProcessorType
import io.axoniq.platform.framework.api.QueryBusInformation
import io.axoniq.platform.framework.api.SerializerInformation
import io.axoniq.platform.framework.api.SetupPayload
import io.axoniq.platform.framework.api.StreamingEventProcessorInformation
import io.axoniq.platform.framework.api.SubscribingProcessorInformation
import io.axoniq.platform.framework.api.SupportedFeatures
import io.axoniq.platform.framework.api.UnspecifiedMessageSourceInformation
import io.axoniq.platform.framework.api.Versions
import io.axoniq.platform.framework.findAllDecoratorsOfType
import io.axoniq.platform.framework.getPropertyType
import io.axoniq.platform.framework.getPropertyValue
import io.axoniq.platform.framework.unwrapPossiblyDecoratedClass
import org.axonframework.common.configuration.Configuration
import org.axonframework.common.util.MavenArtifactVersionResolver
import org.axonframework.conversion.Converter
import org.axonframework.messaging.commandhandling.CommandBus
import org.axonframework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector
import org.axonframework.messaging.commandhandling.interception.InterceptingCommandBus
import org.axonframework.messaging.core.MessageDispatchInterceptor
import org.axonframework.messaging.core.MessageHandlerInterceptor
import org.axonframework.messaging.core.SubscribableEventSource
import org.axonframework.messaging.eventhandling.EventSink
import org.axonframework.messaging.eventhandling.processing.EventProcessor
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.MaxSegmentProvider
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessor
import org.axonframework.messaging.eventstreaming.StreamableEventSource
import org.axonframework.messaging.eventstreaming.TrackingTokenSource
import org.axonframework.messaging.queryhandling.QueryBus
import org.axonframework.messaging.queryhandling.distributed.PayloadConvertingQueryBusConnector
import org.axonframework.messaging.queryhandling.distributed.QueryBusConnector
import org.axonframework.messaging.queryhandling.interception.InterceptingQueryBus
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount

class SetupPayloadCreator(
        private val configuration: Configuration,
) {

    fun createReport(): SetupPayload {
        return SetupPayload(
                commandBus = commandBusInformation(),
                queryBus = queryBusInformation(),
                eventStore = eventBusInformation(),
                processors = configuration.getComponents(EventProcessor::class.java).mapNotNull {
                    toProcessor(it.value)
                },
                versions = versionInformation(),
                upcasters = emptyList(),
                features = SupportedFeatures(
                        heartbeat = true,
                        threadDump = true,
                )
        )
    }

    private fun toProcessor(processor: EventProcessor): EventProcessorInformation? {
        return when (processor) {
            is PooledStreamingEventProcessor -> toPooledStreamingProcessorInformation(processor)
            is SubscribingEventProcessor -> toSubscribingProcessorInformation(processor)
            else -> null
        }
    }

    private fun toPooledStreamingProcessorInformation(processor: PooledStreamingEventProcessor): EventProcessorInformation {
        val configuration = processor.getPropertyValue<PooledStreamingEventProcessorConfiguration>("configuration")
        val eventSource = processor.getPropertyValue<StreamableEventSource>("eventSource")
                ?.unwrapPossiblyDecoratedClass(StreamableEventSource::class.java)

        return EventProcessorInformation(
                name = processor.name(),
                processorType = ProcessorType.POOLED_STREAMING,
                commonProcessorInformation = CommonProcessorInformation(
                        messageSource = createSourceInformation(eventSource),
                        errorHandler = null,
                        invocationErrorHandler = null,
                        interceptors = listOf(),
                ),
                streamingInformation = streamingEventProcessorInformation(processor, configuration),
                pooledStreamingInformation = PooledStreamingEventProcessorInformation(
                        maxClaimedSegments = configuration?.getPropertyValue<MaxSegmentProvider>("maxSegmentProvider")
                                ?.apply(processor.name()),
                        claimExtensionThreshold = configuration?.getPropertyValue("claimExtensionThreshold"),
                        coordinatorExtendsClaims = configuration?.getPropertyValue("coordinatorExtendsClaims")
                ),
        )
    }

    private fun createSourceInformation(eventSource: StreamableEventSource?): MessageSourceInformation {
        if(eventSource == null) {
            return UnspecifiedMessageSourceInformation("unknown")
        }
        if(eventSource::class.simpleName == "StorageEngineBackedEventStore") {
            val storageEngine = eventSource.getPropertyValue<Any>("eventStorageEngine")?.unwrapPossiblyDecoratedClass("org.axonframework.eventsourcing.eventstore.EventStorageEngine")
            if(storageEngine != null && storageEngine::class.simpleName == "AxonServerEventStorageEngine") {
                // Woohoo, Axon Server
                val context = storageEngine.getPropertyValue<Any>("connection")?.getPropertyValue<String>("context")
                if (context != null)
                    return AxonServerEventStoreMessageSourceInformation(
                            className = storageEngine::class.qualifiedName!!,
                            contexts = listOf(context)
                    )
            }

            val type = storageEngine?.javaClass?.name ?: "unknown"
            return UnspecifiedMessageSourceInformation(type)
        }
        val type = eventSource.javaClass.name
        return UnspecifiedMessageSourceInformation(type)
    }

    private fun toSubscribingProcessorInformation(processor: SubscribingEventProcessor): EventProcessorInformation {
        return EventProcessorInformation(
                name = processor.name(),
                processorType = ProcessorType.SUBSCRIBING,
                commonProcessorInformation = CommonProcessorInformation(
                        messageSource = UnspecifiedMessageSourceInformation(
                                processor.getPropertyValue<SubscribableEventSource>("eventSource")?.unwrapPossiblyDecoratedClass(SubscribableEventSource::class.java, excludedNames = listOf("eventBus"))?.javaClass?.name
                                        ?: "none"
                        ),
                        errorHandler = null,
                        invocationErrorHandler = null,
                        interceptors = listOf()
                ),
                subscribingProcessorInformation = SubscribingProcessorInformation(
                        processingStrategy = "none"
                )
        )
    }

    private fun streamingEventProcessorInformation(processor: StreamingEventProcessor, configuration: PooledStreamingEventProcessorConfiguration?) =
            StreamingEventProcessorInformation(
                    batchSize = configuration?.getPropertyValue("batchSize"),
                    tokenClaimInterval = configuration?.getPropertyValue("tokenClaimInterval"),
                    tokenStoreType = processor.getPropertyType("tokenStore", TokenStore::class.java),
                    supportsReset = processor.supportsReset(),
                    tokenStoreClaimTimeout = processor.getStoreTokenClaimTimeout("tokenStore"),
            )

    private val dependenciesToCheck = listOf(
            "org.axonframework:axon",
            "org.axonframework:axon-common",
            "org.axonframework:axon-conversion",
            "org.axonframework:axon-eventsourcing",
            "org.axonframework:axon-messaging",
            "org.axonframework:axon-modelling",
            "org.axonframework:axon-server-connector",
            "org.axonframework:axon-test",
            "org.axonframework:axon-update",
            "org.axonframework:axon-legacy",
            "org.axonframework:axon-legacy-aggregate",
            "org.axonframework:axon-legacy-saga",
            "org.axonframework:axon-migration",
            "org.axonframework:axon-integrationtests",
            "org.axonframework:axon-todo",
            "org.axonframework:axon-coverage-report",
            "org.axonframework.extensions:axon-extensions",
            "org.axonframework.extensions.metrics:axon-metrics-extension",
            "org.axonframework.extensions.tracing:axon-tracing-extension",
            "org.axonframework.extensions.spring:axon-spring-extension",
            "org.axonframework.extensions.metrics:axon-metrics-micrometer",
            "org.axonframework.extensions.metrics:axon-metrics-dropwizard",
            "org.axonframework.extensions.tracing:axon-tracing-opentelemetry",
            "org.axonframework.extensions.spring:axon-spring",
            "org.axonframework.extensions.spring:axon-spring-boot-autoconfigure",
            "org.axonframework.extensions.spring:axon-spring-boot-starter",
            "io.axoniq.framework:postgresql",
            "io.axoniq.framework:postgresql-cor",
            "io.axoniq:axonserver-connector-java",
            "io.axoniq.platform:axoniq-platform-client-messaging",
            "io.axoniq.platform:axoniq-platform-client-modelling",
            "io.axoniq.platform:axoniq-platform-client-eventsourcing",
            "io.axoniq.platform:axoniq-platform-client-spring-boot-starter",
    )

    private fun versionInformation(): Versions {
        return Versions(
                frameworkVersion = resolveVersion("org.axonframework:axon-messaging") ?: "Unknown",
                moduleVersions = dependenciesToCheck.map {
                    ModuleVersion(
                            it,
                            resolveVersion(it)
                    )
                }
        )
    }

    private fun resolveVersion(dep: String): String? {
        val (groupId, artifactId) = dep.split(":")
        return MavenArtifactVersionResolver(
                groupId,
                artifactId,
                this::class.java.classLoader
        ).get()
    }

    private fun queryBusInformation(): QueryBusInformation {
        val busComponent = configuration.getComponent(QueryBus::class.java)
        val bus = busComponent.unwrapPossiblyDecoratedClass(QueryBus::class.java, listOf("localSegment"))
        val isDistributed = bus::class.java.name == "org.axonframework.messaging.queryhandling.distributed.DistributedQueryBus"
        val connector = bus.getPropertyValue<QueryBusConnector>("connector")?.unwrapPossiblyDecoratedClass(QueryBusConnector::class.java)
        val axonServer = if (isDistributed && connector != null) {
            connector::class.java.name == "org.axonframework.axonserver.connector.query.AxonServerQueryBusConnector"
        } else false
        val localSegmentType = if (axonServer) bus.getPropertyType("localSegment", QueryBus::class.java) else null
        val context = extractContext(connector)

        val handlerInterceptors = extractInterceptors(busComponent, InterceptingQueryBus::class.java, "handlerInterceptors")
        val dispatchInterceptors = extractInterceptors(busComponent, InterceptingQueryBus::class.java, "dispatchInterceptors")

        val payloadConvertingConnector = bus.getPropertyValue<QueryBusConnector>("connector")?.findAllDecoratorsOfType(PayloadConvertingQueryBusConnector::class.java)?.firstOrNull()
        val converter = payloadConvertingConnector?.getPropertyValue<Converter>("converter")

        return QueryBusInformation(
                type = bus::class.java.name + if (connector != null) " (${connector::class.java.simpleName})" else "",
                axonServer = axonServer,
                localSegmentType = localSegmentType,
                context = context,
                handlerInterceptors = handlerInterceptors,
                dispatchInterceptors = dispatchInterceptors,
                messageSerializer = null,
                serializer = unwrapConverter(converter),
        )
    }

    private fun eventBusInformation(): EventStoreInformation {
        val bus = configuration.getComponent(EventSink::class.java).unwrapPossiblyDecoratedClass(EventSink::class.java, listOf("eventBus"))
        val storageEngine = bus.getPropertyValue<Any>("eventStorageEngine")?.unwrapPossiblyDecoratedClass("org.axonframework.eventsourcing.eventstore.EventStorageEngine")
        val axonServer = (storageEngine?.javaClass?.simpleName ?: "") in listOf("AxonServerEventStorageEngine", "AggregateBasedAxonServerEventStorageEngine")
        val context = if(axonServer) {
            storageEngine?.getPropertyValue<Any>("connection")?.getPropertyValue<String>("context")
        } else null
        var converter = storageEngine?.getPropertyValue<Any>("converter")
        if(converter != null && converter::class.java.name == "org.axonframework.axonserver.connector.event.TaggedEventConverter") {
            converter = converter.getPropertyValue<Converter>("converter")
        }
        val realConverter = if(converter is Converter) {
            converter.unwrapPossiblyDecoratedClass(Converter::class.java, excludedTypes = listOf(
                    "org.axonframework.conversion.ChainingContentTypeConverter"
            ))?.javaClass?.name
        } else null
        return EventStoreInformation(
                type = bus::class.java.name + if(storageEngine != null) " (${storageEngine::class.java.simpleName})" else "",
                axonServer = axonServer,
                context = context,
                dispatchInterceptors = emptyList(),
                eventSerializer = realConverter?.let { SerializerInformation(type = it, false) },
                snapshotSerializer = null,
                approximateSize = if(storageEngine is TrackingTokenSource) {
                    storageEngine.latestToken(null).get()?.position()?.orElse(-1) ?: -1
                } else null
        )
    }

    private fun commandBusInformation(): CommandBusInformation {
        val busComponent = configuration.getComponent(CommandBus::class.java)
        val bus = busComponent.unwrapPossiblyDecoratedClass(CommandBus::class.java, listOf("localSegment"))
        val isDistributed = bus::class.java.name == "org.axonframework.messaging.commandhandling.distributed.DistributedCommandBus"
        val connector = bus.getPropertyValue<Any>("connector")?.unwrapPossiblyDecoratedClass("org.axonframework.messaging.commandhandling.distributed.CommandBusConnector")
        val axonServer = if (isDistributed && connector != null) {
            connector::class.java.name == "org.axonframework.axonserver.connector.command.AxonServerCommandBusConnector"
        } else false
        val localSegmentType = if (axonServer) bus.getPropertyType("localSegment", CommandBus::class.java) else null
        val context = extractContext(connector)

        val handlerInterceptors = extractInterceptors(busComponent, InterceptingCommandBus::class.java, "handlerInterceptors")
        val dispatchInterceptors = extractInterceptors(busComponent, InterceptingCommandBus::class.java, "dispatchInterceptors")

        val payloadConvertingConnector = bus.getPropertyValue<Any>("connector")?.findAllDecoratorsOfType(PayloadConvertingCommandBusConnector::class.java)?.firstOrNull()
        val converter = payloadConvertingConnector?.getPropertyValue<Any>("converter")

        return CommandBusInformation(
                type = bus::class.java.name + if (connector != null) " (${connector::class.java.simpleName})" else "",
                axonServer = axonServer,
                localSegmentType = localSegmentType,
                context = context,
                handlerInterceptors = handlerInterceptors,
                dispatchInterceptors = dispatchInterceptors,
                messageSerializer = unwrapConverter(converter)
        )
    }

    private fun Any.getStoreTokenClaimTimeout(fieldName: String): Long? = getPropertyValue<Any>(fieldName)
            ?.getPropertyValue<TemporalAmount>("claimTimeout")?.let { it.get(ChronoUnit.SECONDS) * 1000 }

    private fun <T : Any> extractInterceptors(
            busComponent: T,
            interceptingBusClass: Class<*>,
            propertyName: String
    ): List<InterceptorInformation> {
        return busComponent.findAllDecoratorsOfType(interceptingBusClass)
                .flatMap {
                    it.getPropertyValue<List<*>>(propertyName) ?: emptyList()
                }
                .map { InterceptorInformation(type = it!!::class.java.name, measured = false) }
                .filter { !it.type.contains("DefaultHandlerInterceptorRegistry") }
    }

    private fun extractContext(connector: Any?): String? {
        return connector?.getPropertyValue<Any>("connection")?.getPropertyValue<String>("context")
    }

    private fun unwrapConverter(converter: Any?): SerializerInformation? {
        return converter?.unwrapPossiblyDecoratedClass(Converter::class.java, excludedTypes = listOf(
                "org.axonframework.conversion.ChainingContentTypeConverter"
        ))?.javaClass?.simpleName?.let { SerializerInformation(type = it, false) }
    }

}


