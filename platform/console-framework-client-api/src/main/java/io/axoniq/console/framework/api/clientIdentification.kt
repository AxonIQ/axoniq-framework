/*
 * Copyright (c) 2022-2023. AxonIQ B.V.
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

package io.axoniq.console.framework.api

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.net.URLDecoder
import java.net.URLEncoder

/***
 * Represents the authentication of a client to AxonIQ Console.
 * Contains an accessToken which should be validated upon acceptance of the connection.
 *
 * The token looks like the following:
 * @code Bearer WORK_SPACE_ID:ENVIRONMENT_ID:COMPONENT_NAME:NODE_ID:ACCESS_TOKEN}
 */
data class ConsoleClientAuthentication(
        val identification: ConsoleClientIdentifier,
        val accessToken: String,
) {
    fun toBearerToken(): String {
        return BEARER_PREFIX + listOf(
                identification.environmentId,
                identification.applicationName.encode(),
                identification.nodeName.encode(),
                accessToken,
        ).joinToString(separator = ":")
    }

    companion object {
        private const val BEARER_PREFIX: String = "Bearer "
        private const val TOKEN_ERROR: String = "Not a valid Bearer token!"

        fun fromToken(token: String): ConsoleClientAuthentication {
            assert(token.startsWith(BEARER_PREFIX)) { TOKEN_ERROR }

            val tokenParts = token.removePrefix(BEARER_PREFIX).split(":")
            if (tokenParts.size == 5) {
                val (_, environmentId, applicationName, nodeName, accessToken) = tokenParts
                return ConsoleClientAuthentication(
                        ConsoleClientIdentifier(
                                environmentId = environmentId,
                                applicationName = applicationName.decode(),
                                nodeName = nodeName.decode()
                        ),
                        accessToken
                )
            }

            assert(tokenParts.size == 4) { TOKEN_ERROR }
            val (environmentId, applicationName, nodeName, accessToken) = tokenParts
            return ConsoleClientAuthentication(
                    ConsoleClientIdentifier(
                            environmentId = environmentId,
                            applicationName = applicationName.decode(),
                            nodeName = nodeName.decode()
                    ),
                    accessToken
            )
        }

        fun String.encode(): String = URLEncoder.encode(this, "UTF-8")
        private fun String.decode(): String = URLDecoder.decode(this, "UTF-8")
    }
}

data class ConsoleClientIdentifier(
        val environmentId: String,
        val applicationName: String,
        val nodeName: String,
)

data class SetupPayload(
        val features: SupportedFeatures? = SupportedFeatures(),
        val commandBus: CommandBusInformation,
        val queryBus: QueryBusInformation,
        val eventStore: EventStoreInformation,
        val processors: List<EventProcessorInformation>,
        val versions: Versions,
        val upcasters: List<String>,
)

data class SupportedFeatures(
        val heartbeat: Boolean? = false,
)

data class Versions(
        val frameworkVersion: String,
        val moduleVersions: List<ModuleVersion>
)

data class ModuleVersion(
        val dependency: String,
        val version: String?,
)

data class CommandBusInformation(
        val type: String,
        val axonServer: Boolean,
        val localSegmentType: String?,
        val context: String?,
        val handlerInterceptors: List<InterceptorInformation> = emptyList(),
        val dispatchInterceptors: List<InterceptorInformation> = emptyList(),
        val messageSerializer: SerializerInformation?,
)

data class QueryBusInformation(
        val type: String,
        val axonServer: Boolean,
        val localSegmentType: String?,
        val context: String?,
        val handlerInterceptors: List<InterceptorInformation> = emptyList(),
        val dispatchInterceptors: List<InterceptorInformation> = emptyList(),
        val messageSerializer: SerializerInformation?,
        val serializer: SerializerInformation?,
)

data class EventStoreInformation(
        val type: String,
        val axonServer: Boolean,
        val context: String?,
        val dispatchInterceptors: List<InterceptorInformation> = emptyList(),
        val eventSerializer: SerializerInformation?,
        val snapshotSerializer: SerializerInformation?,
        val approximateSize: Long? = null,
)


data class EventProcessorInformation(
        val name: String,
        val processorType: ProcessorType? = null,

        val commonProcessorInformation: CommonProcessorInformation? = null,
        val subscribingProcessorInformation: SubscribingProcessorInformation? = null,
        val streamingInformation: StreamingEventProcessorInformation? = null,
        val trackingInformation: TrackingEventProcessorInformation? = null,
        val pooledStreamingInformation: PooledStreamingEventProcessorInformation? = null,

        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val messageSourceType: String? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val contexts: List<String>? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val tokenStoreType: String? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val supportsReset: Boolean? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val batchSize: Int? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val tokenClaimInterval: Long? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val tokenStoreClaimTimeout: Long? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val errorHandler: String? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val invocationErrorHandler: String? = null,
        @Deprecated("Deprecated since version 1.6.0 due to new processor structure")
        val interceptors: List<InterceptorInformation>? = null,
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
        JsonSubTypes.Type(value = AxonServerEventStoreMessageSourceInformation::class, name = "AxonServer"),
        JsonSubTypes.Type(value = EmbeddedEventStoreMessageSourceInformation::class, name = "Embedded"),
        JsonSubTypes.Type(value = MultiStreamableMessageSourceInformation::class, name = "MultiStreamable"),
        JsonSubTypes.Type(value = PersistentStreamMessageSourceInformation::class, name = "PersistentStream"),
        JsonSubTypes.Type(value = UnspecifiedMessageSourceInformation::class, name = "Unspecified"),
)
interface MessageSourceInformation {
    val className: String
}

data class AxonServerEventStoreMessageSourceInformation(
        override val className: String,
        val contexts: List<String>
) : MessageSourceInformation

data class EmbeddedEventStoreMessageSourceInformation(
        override val className: String,
        val optimizeEventConsumption: Boolean?,
        val fetchDelay: Long?,
        val cachedEvents: Int?,
        val cleanupDelay: Long?,
        val eventStorageEngineType: String?,
) : MessageSourceInformation

data class MultiStreamableMessageSourceInformation(
        override val className: String,
        val sources: List<MessageSourceInformation>,
) : MessageSourceInformation

data class PersistentStreamMessageSourceInformation(
        override val className: String,
        val context: String?,
        val streamIdentifier: String?,
        val batchSize: Int?,
        val sequencingPolicy: String?,
        val sequencingPolicyParameters: String?,
        val query: String?,
) : MessageSourceInformation

data class UnspecifiedMessageSourceInformation(
        override val className: String,
) : MessageSourceInformation

enum class ProcessorType {
    SUBSCRIBING,
    TRACKING,
    POOLED_STREAMING,
}

data class StreamingEventProcessorInformation(
        val tokenStoreType: String?,
        val batchSize: Int?,
        val tokenClaimInterval: Long?,
        val tokenStoreClaimTimeout: Long?,
        val supportsReset: Boolean?,
)

data class TrackingEventProcessorInformation(
        val maxThreadCount: Int?,
        val eventAvailabilityTimeout: Int?,
        val storeTokenBeforeProcessing: Boolean?,
)

data class PooledStreamingEventProcessorInformation(
        val maxClaimedSegments: Int?,
        val claimExtensionThreshold: Long?,
        val coordinatorExtendsClaims: Boolean?,
)

data class SubscribingProcessorInformation(
        val processingStrategy: String,
)

data class CommonProcessorInformation(
        val messageSource: MessageSourceInformation?,
        val errorHandler: String?,
        val invocationErrorHandler: String?,
        val interceptors: List<InterceptorInformation>,
)

data class InterceptorInformation(
        val type: String,
        val measured: Boolean,
)

data class SerializerInformation(
        val type: String,
        val grpcAware: Boolean,
)
