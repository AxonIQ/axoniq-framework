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
    val commandBus: CommandBusInformation,
    val queryBus: QueryBusInformation,
    val eventStore: EventStoreInformation,
    val processors: List<ProcessorInformation>,
    val versions: Versions,
    val upcasters: List<String>,
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
    val handlerInterceptors: List<io.axoniq.console.framework.api.InterceptorInformation> = emptyList(),
    val dispatchInterceptors: List<io.axoniq.console.framework.api.InterceptorInformation> = emptyList(),
    val messageSerializer: io.axoniq.console.framework.api.SerializerInformation?,
)

data class QueryBusInformation(
    val type: String,
    val axonServer: Boolean,
    val localSegmentType: String?,
    val context: String?,
    val handlerInterceptors: List<io.axoniq.console.framework.api.InterceptorInformation> = emptyList(),
    val dispatchInterceptors: List<io.axoniq.console.framework.api.InterceptorInformation> = emptyList(),
    val messageSerializer: io.axoniq.console.framework.api.SerializerInformation?,
    val serializer: io.axoniq.console.framework.api.SerializerInformation?,
)

data class EventStoreInformation(
    val type: String,
    val axonServer: Boolean,
    val context: String?,
    val dispatchInterceptors: List<io.axoniq.console.framework.api.InterceptorInformation> = emptyList(),
    val eventSerializer: io.axoniq.console.framework.api.SerializerInformation?,
    val snapshotSerializer: io.axoniq.console.framework.api.SerializerInformation?,
)

data class ProcessorInformation(
    val name: String,
    val messageSourceType: String,
    val contexts: List<String>? = emptyList(),
    val tokenStoreType: String,
    val supportsReset: Boolean,
    val batchSize: Int,
    val tokenClaimInterval: Long,
    val tokenStoreClaimTimeout: Long,
    val errorHandler: String,
    val invocationErrorHandler: String,
    val interceptors: List<io.axoniq.console.framework.api.InterceptorInformation>,
)

data class InterceptorInformation(
        val type: String,
        val measured: Boolean,
)

data class SerializerInformation(
        val type: String,
        val grpcAware: Boolean,
)
