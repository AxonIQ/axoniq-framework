/*
 * Copyright (c) 2022-2024. AxonIQ B.V.
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

package io.axoniq.console.framework.client

import io.axoniq.console.framework.api.*
import io.axoniq.console.framework.unwrapPossiblyDecoratedClass
import org.axonframework.commandhandling.CommandBus
import org.axonframework.common.ReflectionUtils
import org.axonframework.config.Configuration
import org.axonframework.config.EventProcessingModule
import org.axonframework.eventhandling.*
import org.axonframework.eventhandling.pooled.PooledStreamingEventProcessor
import org.axonframework.eventhandling.tokenstore.TokenStore
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.messaging.StreamableMessageSource
import org.axonframework.messaging.SubscribableMessageSource
import org.axonframework.queryhandling.QueryBus
import org.axonframework.serialization.upcasting.Upcaster
import org.axonframework.util.MavenArtifactVersionResolver
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount

class SetupPayloadCreator(
        private val configuration: Configuration,
) {
    private val eventProcessingConfiguration = configuration.eventProcessingConfiguration() as EventProcessingModule

    fun createReport(): SetupPayload {
        val processors = eventProcessingConfiguration.eventProcessors().keys
        return SetupPayload(
                commandBus = commandBusInformation(),
                queryBus = queryBusInformation(),
                eventStore = eventBusInformation(),
                processors = processors.mapNotNull {
                    toProcessor(it)
                },
                versions = versionInformation(),
                upcasters = upcasters(),
                features = SupportedFeatures(
                        heartbeat = true
                )
        )
    }

    private fun toProcessor(name: String): EventProcessorInformation? {
        val processor = eventProcessingConfiguration.eventProcessor(name, EventProcessor::class.java).orElse(null)
        return when (processor) {
            is PooledStreamingEventProcessor -> toPooledStreamingProcessorInformation(processor)
            is TrackingEventProcessor -> toTrackingProcessorInformation(processor)
            is SubscribingEventProcessor -> toSubscribingProcessorInformation(processor)
            else -> null
        }
    }

    private fun toPooledStreamingProcessorInformation(processor: PooledStreamingEventProcessor): EventProcessorInformation {
        return EventProcessorInformation(
                name = processor.name,
                processorType = ProcessorType.POOLED_STREAMING,
                commonProcessorInformation = commonProcessorInformation(
                        processor,
                        mapStreamableMessageSource(processor, processor.getPropertyValue("messageSource"))
                ),
                streamingInformation = streamingEventProcessorInformation(processor),
                pooledStreamingInformation = PooledStreamingEventProcessorInformation(
                        maxClaimedSegments = processor.getPropertyValue("maxClaimedSegments"),
                        claimExtensionThreshold = processor.getPropertyValue("claimExtensionThreshold"),
                        coordinatorExtendsClaims = processor.getPropertyValue<Any>("coordinator")?.getPropertyValue("coordinatorExtendsClaims")
                ),
        )
    }

    private fun toTrackingProcessorInformation(processor: TrackingEventProcessor): EventProcessorInformation {
        return EventProcessorInformation(
                name = processor.name,
                processorType = ProcessorType.TRACKING,
                commonProcessorInformation = commonProcessorInformation(
                        processor,
                        mapStreamableMessageSource(processor, processor.getPropertyValue("messageSource"))
                ),
                streamingInformation = streamingEventProcessorInformation(processor),
                trackingInformation = TrackingEventProcessorInformation(
                        maxThreadCount = processor.getPropertyValue("maxThreadCount"),
                        eventAvailabilityTimeout = processor.getPropertyValue("eventAvailabilityTimeout"),
                        storeTokenBeforeProcessing = processor.getPropertyValue("storeTokenBeforeProcessing"),
                ),
        )
    }

    private fun toSubscribingProcessorInformation(processor: SubscribingEventProcessor): EventProcessorInformation {
        return EventProcessorInformation(
                name = processor.name,
                processorType = ProcessorType.SUBSCRIBING,
                commonProcessorInformation = commonProcessorInformation(
                        processor,
                        mapSubscribableMessageSource(processor.getPropertyValue("messageSource"))
                ),
                subscribingProcessorInformation = SubscribingProcessorInformation(
                        processingStrategy = processor.getPropertyType("processingStrategy")
                )
        )
    }

    private fun commonProcessorInformation(processor: EventProcessor, messageSourceInfo: MessageSourceInformation) =
            CommonProcessorInformation(
                    messageSource = messageSourceInfo,
                    errorHandler = eventProcessingConfiguration.errorHandler(processor.name)::class.java.name,
                    invocationErrorHandler = eventProcessingConfiguration.listenerInvocationErrorHandler(processor.name)::class.java.name,
                    interceptors = processor.getInterceptors("interceptors"),
            )

    private fun mapStreamableMessageSource(processor: EventProcessor, messageSource: StreamableMessageSource<*>?): MessageSourceInformation {
        if (messageSource == null) {
            return UnspecifiedMessageSourceInformation("Unknown")
        }
        val unwrapped = messageSource.unwrapPossiblyDecoratedClass(StreamableMessageSource::class.java)
        return when {
            unwrapped is MultiStreamableMessageSource -> MultiStreamableMessageSourceInformation(
                    messageSource::class.java.name,
                    messageSource
                            .getPropertyValue<List<StreamableMessageSource<*>>>("eventStreams")
                            ?.map { mapStreamableMessageSource(processor, it) }
                            ?: emptyList()
            )
            unwrapped is EmbeddedEventStore -> createEmbeddedMessageSourceInformation(unwrapped)
            unwrapped::class.java.simpleName == "AxonServerEventStore" -> createAxonServerMessageSourceInfoFromStore(
                    unwrapped::class.java.name,
                    unwrapped.getPropertyValue<Any>("storageEngine")?.getPropertyValue<String>("context")
            )
            unwrapped::class.java.simpleName == "AxonServerMessageSource" -> createAxonServerMessageSourceInfoFromMessageSource(unwrapped)
            unwrapped::class.java.simpleName == "AxonIQEventStorageEngine" -> createAxonServerMessageSourceInfoFromStorageEngine(unwrapped)
            else -> UnspecifiedMessageSourceInformation(unwrapped::class.java.name)
        }
    }

    private fun mapSubscribableMessageSource(messageSource: SubscribableMessageSource<*>?): MessageSourceInformation {
        if (messageSource == null) {
            return UnspecifiedMessageSourceInformation("Unknown")
        }
        val unwrapped = messageSource.unwrapPossiblyDecoratedClass(SubscribableMessageSource::class.java)
        return when {
            unwrapped is EmbeddedEventStore -> createEmbeddedMessageSourceInformation(unwrapped)
            unwrapped::class.java.simpleName == "AxonServerEventStore" -> createAxonServerMessageSourceInfoFromStore(
                    unwrapped::class.java.name,
                    unwrapped.getPropertyValue<Any>("storageEngine")?.getPropertyValue<String>("context")
            )
            unwrapped::class.java.simpleName == "PersistentStreamMessageSource" -> createPersistentStreamMessageSourceInfo(unwrapped)
            else -> UnspecifiedMessageSourceInformation(unwrapped::class.java.name)
        }
    }

    private fun streamingEventProcessorInformation(processor: StreamingEventProcessor) = StreamingEventProcessorInformation(
            batchSize = processor.getPropertyValue("batchSize"),
            tokenClaimInterval = processor.getPropertyValue("tokenClaimInterval"),
            tokenStoreType = processor.getPropertyType("tokenStore", TokenStore::class.java),
            supportsReset = processor.supportsReset(),
            tokenStoreClaimTimeout = processor.getStoreTokenClaimTimeout("tokenStore"),
    )

    private fun createEmbeddedMessageSourceInformation(messageSource: EmbeddedEventStore): MessageSourceInformation {
        return EmbeddedEventStoreMessageSourceInformation(
                className = messageSource::class.java.name,
                optimizeEventConsumption = messageSource.getPropertyValue("optimizeEventConsumption"),
                fetchDelay = messageSource.getPropertyValue<Any?>("producer")?.getPropertyValue<Long>("fetchDelayNanos")?.let { it / 1_000_000 },
                cachedEvents = messageSource.getPropertyValue<Any?>("producer")?.getPropertyValue<Int>("cachedEvents"),
                cleanupDelay = messageSource.getPropertyValue("cleanupDelayMillis"),
                eventStorageEngineType = messageSource.getPropertyType("storageEngine")
        )
    }

    private fun createAxonServerMessageSourceInfoFromStore(
            className: String,
            context: String?
    ) = AxonServerEventStoreMessageSourceInformation(
            className,
            listOfNotNull(context)
    )

    private fun createPersistentStreamMessageSourceInfo(
            subscribableMessageSource: SubscribableMessageSource<*>
    ): MessageSourceInformation {
        val className: String = subscribableMessageSource::class.java.name
        val persistentStreamConnection: Any = subscribableMessageSource.getPropertyValue<Any>("persistentStreamConnection")
                ?: return UnspecifiedMessageSourceInformation(className)
        val persistentStreamProperties: Any = persistentStreamConnection.getPropertyValue<Any>("persistentStreamProperties")
                ?: return UnspecifiedMessageSourceInformation(className)

        val context = persistentStreamConnection.getPropertyValue<String>("defaultContext")
        val streamIdentifier = persistentStreamConnection.getPropertyValue<String>("streamId")
        val batchSize = persistentStreamConnection.getPropertyValue<Int>("batchSize")

        val sequencingPolicy = persistentStreamProperties.getPropertyValue<String>("sequencingPolicyName")
        val sequencingPolicyParameters = persistentStreamProperties.getPropertyValue<String>("sequencingPolicyParameters")
        val query = persistentStreamProperties.getPropertyValue<String>("filter")

        return PersistentStreamMessageSourceInformation(
                className = subscribableMessageSource::class.java.name,
                context = context,
                streamIdentifier = streamIdentifier,
                batchSize = batchSize,
                sequencingPolicy = sequencingPolicy,
                sequencingPolicyParameters = sequencingPolicyParameters,
                query = query,
        )
    }

    private fun createAxonServerMessageSourceInfoFromMessageSource(messageSource: StreamableMessageSource<*>): MessageSourceInformation {
        val context = messageSource.getPropertyValue<Any>("eventStorageEngine")?.getPropertyValue<String>("context")

        return AxonServerEventStoreMessageSourceInformation(
                messageSource::class.java.name,
                listOfNotNull(context)
        )
    }

    private fun createAxonServerMessageSourceInfoFromStorageEngine(messageSource: StreamableMessageSource<*>): MessageSourceInformation {
        val context = messageSource.getPropertyValue<String>("context")

        return AxonServerEventStoreMessageSourceInformation(
                messageSource::class.java.name,
                listOfNotNull(context)
        )
    }

    private fun upcasters(): List<String> {
        val upcasters =
                configuration.upcasterChain().getPropertyValue<List<out Upcaster<*>>>("upcasters") ?: emptyList()
        return upcasters.map { it::class.java.name }
    }

    private val dependenciesToCheck = listOf(
            "org.axonframework:axon-messaging",
            "org.axonframework:axon-configuration",
            "org.axonframework:axon-disruptor",
            "org.axonframework:axon-eventsourcing",
            "org.axonframework:axon-legacy",
            "org.axonframework:axon-metrics",
            "org.axonframework:axon-micrometer",
            "org.axonframework:axon-modelling",
            "org.axonframework:axon-server-connector",
            "org.axonframework:axon-spring",
            "org.axonframework:axon-spring-boot-autoconfigure",
            "org.axonframework:axon-spring-boot-starter",
            "org.axonframework:axon-tracing-opentelemetry",
            "org.axonframework.extensions.amqp:axon-amqp",
            "org.axonframework.extensions.jgroups:axon-jgroups",
            "org.axonframework.extensions.kafka:axon-kafka",
            "org.axonframework.extensions.mongo:axon-mongo",
            "org.axonframework.extensions.reactor:axon-reactor",
            "org.axonframework.extensions.springcloud:axon-springcloud",
            "org.axonframework.extensions.tracing:axon-tracing",
            "io.axoniq:axonserver-connector-java",
            "io.axoniq.console:console-framework-client",
    )

    private fun versionInformation(): Versions {
        return Versions(
                frameworkVersion = resolveVersion("org.axonframework:axon-messaging") ?: "Unknown",
                moduleVersions = dependenciesToCheck.map {
                    io.axoniq.console.framework.api.ModuleVersion(
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
        val bus = configuration.queryBus().unwrapPossiblyDecoratedClass(QueryBus::class.java)
        val axonServer = bus::class.java.name == "org.axonframework.axonserver.connector.query.AxonServerQueryBus"
        val localSegmentType = if (axonServer) bus.getPropertyType("localSegment", QueryBus::class.java) else null
        val context = if (axonServer) bus.getPropertyValue<String>("context") else null
        val handlerInterceptors = if (axonServer) {
            bus.getPropertyValue<Any>("localSegment")?.getInterceptors("handlerInterceptors") ?: emptyList()
        } else {
            bus.getInterceptors("handlerInterceptors")
        }
        val dispatchInterceptors = bus.getInterceptors("dispatchInterceptors")
        val messageSerializer = if (axonServer) {
            bus.getPropertyValue<Any>("serializer")?.getSerializerType("messageSerializer")
        } else null
        val serializer = if (axonServer) {
            bus.getPropertyValue<Any>("serializer")?.getSerializerType("serializer")
        } else null
        return QueryBusInformation(
                type = bus::class.java.name,
                axonServer = axonServer,
                localSegmentType = localSegmentType,
                context = context,
                handlerInterceptors = handlerInterceptors,
                dispatchInterceptors = dispatchInterceptors,
                messageSerializer = messageSerializer,
                serializer = serializer,
        )
    }

    private fun eventBusInformation(): EventStoreInformation {
        val bus = configuration.eventBus().unwrapPossiblyDecoratedClass(EventStore::class.java)
        val axonServer =
                bus::class.java.name == "org.axonframework.axonserver.connector.event.axon.AxonServerEventStore"
        val context = if (axonServer) {
            bus.getPropertyValue<Any>("storageEngine")?.getPropertyValue<String>("context")
        } else null
        val dispatchInterceptors = bus.getInterceptors("dispatchInterceptors")
        return EventStoreInformation(
                type = bus::class.java.name,
                axonServer = axonServer,
                context = context,
                dispatchInterceptors = dispatchInterceptors,
                eventSerializer = bus.getPropertyValue<Any>("storageEngine")?.getSerializerType("eventSerializer"),
                snapshotSerializer = bus.getPropertyValue<Any>("storageEngine")?.getSerializerType("snapshotSerializer"),
                approximateSize = getApproximateSize(bus)
        )
    }

    private fun getApproximateSize(bus: EventBus): Long? =
            if (bus is StreamableMessageSource<*>) {
                runCatching {
                    getSizeFromToken(bus.createHeadToken())
                }.getOrElse { null }
            } else {
                null
            }

    private fun getSizeFromToken(token: TrackingToken): Long? =
            when (token) {
                is GlobalSequenceTrackingToken -> token.globalIndex
                is GapAwareTrackingToken -> token.index
                is MultiSourceTrackingToken -> token.trackingTokens.values.sumOf {
                    getSizeFromToken(it) ?: 0
                }

                else -> null
            }

    private fun commandBusInformation(): CommandBusInformation {
        val bus = configuration.commandBus().unwrapPossiblyDecoratedClass(CommandBus::class.java)
        val axonServer = bus::class.java.name == "org.axonframework.axonserver.connector.command.AxonServerCommandBus"
        val localSegmentType =
                if (axonServer) bus.getPropertyType("localSegment", CommandBus::class.java) else null
        val context = if (axonServer) bus.getPropertyValue<String>("context") else null
        val handlerInterceptors = if (axonServer) {
            bus.getPropertyValue<Any>("localSegment")?.getInterceptors("handlerInterceptors", "invokerInterceptors")
                    ?: emptyList()
        } else {
            bus.getInterceptors("handlerInterceptors", "invokerInterceptors")
        }
        val dispatchInterceptors = bus.getInterceptors("dispatchInterceptors")
        val serializer = if (axonServer) {
            bus.getPropertyValue<Any>("serializer")?.getSerializerType("messageSerializer")
        } else null
        return CommandBusInformation(
                type = bus::class.java.name,
                axonServer = axonServer,
                localSegmentType = localSegmentType,
                context = context,
                handlerInterceptors = handlerInterceptors,
                dispatchInterceptors = dispatchInterceptors,
                messageSerializer = serializer
        )
    }

    private fun <T> Any.getPropertyValue(fieldName: String): T? {
        val field = ReflectionUtils.fieldsOf(this::class.java).firstOrNull { it.name == fieldName } ?: return null
        return ReflectionUtils.getMemberValue(
                field,
                this
        )
    }

    private fun Any.getPropertyType(fieldName: String): String {
        return ReflectionUtils.getMemberValue<Any>(
                ReflectionUtils.fieldsOf(this::class.java).first { it.name == fieldName },
                this
        ).let { it::class.java.name }
    }

    private fun Any.getPropertyType(fieldName: String, clazz: Class<out Any>): String {
        return ReflectionUtils.getMemberValue<Any>(
                ReflectionUtils.fieldsOf(this::class.java).first { it.name == fieldName },
                this
        )
                .let { it.unwrapPossiblyDecoratedClass(clazz) }
                .let { it::class.java.name }
    }

    private fun Any.getStoreTokenClaimTimeout(fieldName: String): Long? = getPropertyValue<Any>(fieldName)
            ?.getPropertyValue<TemporalAmount>("claimTimeout")?.let { it.get(ChronoUnit.SECONDS) * 1000 }


    private fun Any.getInterceptors(vararg fieldNames: String): List<InterceptorInformation> {

        val interceptors = fieldNames.firstNotNullOfOrNull { this.getPropertyValue<Any>(it) } ?: return emptyList()
        if (interceptors::class.java.name == "org.axonframework.axonserver.connector.DispatchInterceptors") {
            return interceptors.getInterceptors("dispatchInterceptors")
        }
        if (interceptors !is List<*>) {
            return emptyList()
        }
        return interceptors
                .filterNotNull()
                .map {
                    if (it is AxoniqConsoleMeasuringHandlerInterceptor) {
                        InterceptorInformation(it.subject::class.java.name, true)
                    } else InterceptorInformation(it::class.java.name, false)
                }
                .filter { !it.type.startsWith("org.axonframework.eventhandling") }
    }

    private fun Any.getSerializerType(fieldName: String): SerializerInformation? {
        val serializer = getPropertyValue<Any>(fieldName) ?: return null
        if (serializer::class.java.name == "org.axonframework.axonserver.connector.event.axon.GrpcMetaDataAwareSerializer") {
            return SerializerInformation(serializer.getPropertyType("delegate"), true)
        }
        return SerializerInformation(serializer::class.java.name, false)
    }
}


