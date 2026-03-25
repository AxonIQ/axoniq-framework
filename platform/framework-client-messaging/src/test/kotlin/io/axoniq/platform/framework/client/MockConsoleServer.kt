/*
 * Copyright (c) 2022-2026. AxonIQ B.V.
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

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper
import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.Routes
import io.netty.buffer.ByteBufAllocator
import io.rsocket.Payload
import io.rsocket.RSocket
import io.rsocket.core.RSocketServer
import io.rsocket.exceptions.RejectedSetupException
import io.rsocket.metadata.CompositeMetadata
import io.rsocket.metadata.RoutingMetadata
import io.rsocket.metadata.WellKnownMimeType
import io.rsocket.transport.netty.server.CloseableChannel
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Mono
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * A minimal RSocket server that simulates the Axoniq Platform Console for integration testing.
 *
 * Handles the two routes the client calls during setup and steady state:
 *  - [Routes.Management.SETTINGS_V2] – returns [clientSettings]
 *  - [Routes.Management.HEARTBEAT]   – echoes an empty response
 *
 * After a client connects the server also pushes heartbeats back at [clientSettings].heartbeatInterval
 * so the client's timeout checker stays satisfied.
 */
class MockConsoleServer {

    private val mapper = CBORMapper.builder().build().findAndRegisterModules()
    private var server: CloseableChannel? = null

    @Volatile
    private var activeClientRSocket: RSocket? = null
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private var heartbeatTask: ScheduledFuture<*>? = null

    /** When true the server rejects the setup with "Access Denied". */
    var rejectSetup = false

    /** Settings returned on every SETTINGS_V2 request. */
    var clientSettings: ClientSettingsV2 = ClientSettingsV2(
        heartbeatInterval = 500,
        heartbeatTimeout = 3000,
        processorReportInterval = 5000,
        handlerReportInterval = 5000,
        applicationReportInterval = 5000,
    )

    val port: Int
        get() = (server!!.address() as InetSocketAddress).port

    fun start() {
        server = RSocketServer.create { _, clientRSocket ->
            if (rejectSetup) {
                return@create Mono.error(RejectedSetupException("Access Denied"))
            }
            activeClientRSocket = clientRSocket
            startSendingHeartbeats()
            Mono.just(object : RSocket {
                override fun requestResponse(payload: Payload): Mono<Payload> = handleRequest(payload)
            })
        }
            .bind(TcpServerTransport.create("0.0.0.0", 0))
            .block()!!
    }

    // ---- control methods used by tests ----

    fun startSendingHeartbeats() {
        heartbeatTask?.cancel(true)
        heartbeatTask = scheduler.scheduleWithFixedDelay(
            { sendHeartbeatToClient().subscribe({}, {}) },
            clientSettings.heartbeatInterval,
            clientSettings.heartbeatInterval,
            TimeUnit.MILLISECONDS,
        )
    }

    fun stopSendingHeartbeats() {
        heartbeatTask?.cancel(true)
        heartbeatTask = null
    }

    /** Disposes the current client connection, simulating a server-side close. */
    fun disconnectClients() {
        stopSendingHeartbeats()
        activeClientRSocket?.dispose()
        activeClientRSocket = null
    }

    fun stop() {
        stopSendingHeartbeats()
        scheduler.shutdown()
        server?.dispose()
    }

    // ---- internals ----

    private fun handleRequest(payload: Payload): Mono<Payload> {
        val route = extractRoute(payload)
        return when (route) {
            Routes.Management.SETTINGS_V2 -> Mono.just(encodeResponse(clientSettings))
            Routes.Management.HEARTBEAT -> Mono.just(encodeResponse(""))
            else -> Mono.error(IllegalArgumentException("MockConsoleServer: unknown route '$route'"))
        }
    }

    private fun sendHeartbeatToClient(): Mono<Payload> {
        val clientSocket = activeClientRSocket ?: return Mono.empty()
        val metadata = ByteBufAllocator.DEFAULT.compositeBuffer()
        metadata.addRouteMetadata(Routes.Management.HEARTBEAT)
        val metadataBytes = ByteArray(metadata.readableBytes()).also { metadata.readBytes(it) }
        metadata.release()
        return clientSocket.requestResponse(DefaultPayload.create(mapper.writeValueAsBytes(""), metadataBytes))
    }

    /**
     * Creates a response payload with a heap-backed data buffer.
     * [DefaultPayload.create(byte[])] wraps the byte array in an UnpooledHeapByteBuf, which supports
     * [io.netty.buffer.ByteBuf.array] — required by [io.axoniq.platform.framework.client.strategy.CborJackson2EncodingStrategy.decode].
     */
    private fun encodeResponse(obj: Any): Payload = DefaultPayload.create(mapper.writeValueAsBytes(obj))

    private fun extractRoute(payload: Payload): String {
        val compositeMetadata = CompositeMetadata(payload.metadata(), false)
        for (entry in compositeMetadata) {
            if (entry.mimeType == WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.string) {
                return RoutingMetadata(entry.content).iterator().next()
            }
        }
        throw IllegalArgumentException("MockConsoleServer: no routing metadata in payload")
    }
}
