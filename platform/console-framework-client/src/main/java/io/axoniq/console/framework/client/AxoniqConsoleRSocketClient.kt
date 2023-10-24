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

package io.axoniq.console.framework.client

import io.axoniq.console.framework.api.Routes
import io.axoniq.console.framework.client.strategy.RSocketPayloadEncodingStrategy
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.rsocket.Payload
import io.rsocket.RSocket
import io.rsocket.core.RSocketConnector
import io.rsocket.metadata.WellKnownMimeType
import io.rsocket.transport.netty.client.TcpClientTransport
import org.axonframework.lifecycle.Lifecycle
import org.axonframework.lifecycle.Phase
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.netty.tcp.TcpClient
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlin.math.pow

/**
 * The beating heart of the Console client. This class is responsible for connecting to the Console, and keeping
 * the connection alive. It will also ensure that the connection is re-established in case of a disconnect.
 *
 * The server will send heartbeats to the client every 10 seconds. Upon not receiving heartbeats for 32 seconds
 * (missing three of them) we can be sure the connection is dead, and the connection is killed.
 * The client will send heartbeats every 10 seconds as well. It's up to the server to decide how lenient it is in terms
 * of missing heartbeats.
 */
@Suppress("MemberVisibilityCanBePrivate")
class AxoniqConsoleRSocketClient(
    private val environmentId: String,
    private val accessToken: String,
    private val applicationName: String,
    private val host: String,
    private val port: Int,
    private val secure: Boolean,
    private val initialDelay: Long,
    private val setupPayloadCreator: SetupPayloadCreator,
    private val registrar: RSocketHandlerRegistrar,
    private val encodingStrategy: RSocketPayloadEncodingStrategy,
    private val executor: ScheduledExecutorService,
    private val nodeName: String,
    private val heartbeatTimeout: Long = 30
) : Lifecycle {
    private var maintenanceTask: ScheduledFuture<*>? = null
    private val logger = LoggerFactory.getLogger(this::class.java)

    private var rsocket: RSocket? = null
    private var lastSentHeartbeat = Instant.EPOCH
    private var lastReceivedHeartbeat = Instant.now()
    private var lastConnectionTry = Instant.EPOCH
    private var connectionRetryCount = 0

    init {
        registrar.registerHandlerWithoutPayload(Routes.Management.HEARTBEAT) {
            logger.debug("Received heartbeat from AxonIQ Console. Last one was: {}", lastReceivedHeartbeat)
            lastReceivedHeartbeat = Instant.now()
            lastReceivedHeartbeat
        }
    }

    override fun registerLifecycleHandlers(registry: Lifecycle.LifecycleRegistry) {
        registry.onStart(Phase.EXTERNAL_CONNECTIONS, this::start)
        registry.onShutdown(Phase.EXTERNAL_CONNECTIONS, this::dispose)
    }

    /**
     * Sends a message to the AxonIQ Console. If there is no connection active, does nothing silently.
     * The connection will automatically be setup. Losing a few reports is no problem.
     */
    fun send(route: String, payload: Any): Mono<Void> {
        return rsocket
                ?.requestResponse(encodingStrategy.encode(payload, createRoutingMetadata(route)))
                ?.then()
                ?: Mono.empty()
    }

    /**
     * Starts the connection, and starts the maintenance task.
     * The task will ensure that if heartbeats are missed the connection is killed, as well as re-setup in case
     * the connection was lost. The task will do so with an exponential backoff with factor 2, up to a maximum of
     * 60 seconds.
     */
    fun start() {
        if(this.maintenanceTask != null) {
            return
        }
        this.lastReceivedHeartbeat = Instant.now()
        this.maintenanceTask = executor.scheduleWithFixedDelay(
                this::ensureConnectedAndAlive,
                initialDelay,
                1000, TimeUnit.MILLISECONDS
        )
    }

    private fun ensureConnectedAndAlive() {
        if (isConnected()) {
            if (!isAlive()) {
                logger.info("Haven't received a heartbeat for {} seconds from AxonIQ Console. Reconnecting...", ChronoUnit.SECONDS.between(lastReceivedHeartbeat, Instant.now()))
                rsocket?.dispose()
                rsocket = null
            } else if(lastSentHeartbeat < Instant.now().minusSeconds(10)) {
                logger.debug("Sending heartbeat to AxonIQ Console")
                lastSentHeartbeat = Instant.now()
                sendHeartbeat().subscribe()
            }
        }
        if (!isConnected()) {
            val secondsToWaitForReconnect = BACKOFF_FACTOR.pow(connectionRetryCount.toDouble()).coerceAtMost(60.0)
            if (ChronoUnit.SECONDS.between(lastConnectionTry, Instant.now()) < secondsToWaitForReconnect) {
                return
            }
            logger.info("Reconnecting to AxonIQ Console...")
            lastReceivedHeartbeat = Instant.now()
            connectSafely()
        }
    }

    private fun connectSafely() {
        try {
            rsocket = createRSocket()

            // Send a heartbeat to ensure it's set up correctly. Rsocket initializes lazily, so sending a call will do it.
            sendHeartbeat().block()
            logger.info("Connection to AxonIQ Console set up successfully!")
        } catch (e: Exception) {
            disposeRsocket()
            logger.info("Failed to connect to AxonIQ Console", e)
        }
    }

    private fun createRSocket(): RSocket {
        val authentication = io.axoniq.console.framework.api.ConsoleClientAuthentication(
            identification = io.axoniq.console.framework.api.ConsoleClientIdentifier(
                environmentId = environmentId,
                applicationName = applicationName,
                nodeName = nodeName
            ),
            accessToken = accessToken
        )

        val setupPayload = encodingStrategy.encode(
                setupPayloadCreator.createReport(),
                createSetupMetadata(authentication)
        )
        val rsocket = RSocketConnector.create()
            .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.string)
            .dataMimeType(encodingStrategy.getMimeType().string)
            .setupPayload(setupPayload)
            .acceptor { _, rsocket ->
                Mono.just(registrar.createRespondingRSocketFor(rsocket))
            }
            .connect(tcpClientTransport())
            .block()!!
        return rsocket
    }

    private fun sendHeartbeat(): Mono<Payload> {
        return rsocket
                ?.requestResponse(encodingStrategy.encode("", createRoutingMetadata(Routes.Management.HEARTBEAT)))
                ?.doOnError {
                    if (it.message?.contains("Access Denied") == true) {
                        logger.info("Was unable to send call to AxonIQ Console since authentication was incorrect!")
                    }
                }
                ?.doOnSuccess {
                    logger.debug("Heartbeat successfully sent to AxonIQ Console")
                }
                ?: Mono.empty()
    }

    private fun createRoutingMetadata(route: String): CompositeByteBuf {
        val metadata: CompositeByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer()
        metadata.addRouteMetadata(route)
        return metadata
    }

    private fun createSetupMetadata(auth: io.axoniq.console.framework.api.ConsoleClientAuthentication): CompositeByteBuf {
        val metadata: CompositeByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer()
        metadata.addRouteMetadata("client")
        metadata.addAuthMetadata(auth)
        return metadata
    }

    private fun tcpClientTransport() =
        TcpClientTransport.create(tcpClient())

    private fun tcpClient(): TcpClient {
        val client = TcpClient.create()
            .host(host)
            .port(port)
            .doOnDisconnected {
                disposeRsocket()
            }
        return if (secure) {
            return client.secure()
        } else client
    }

    fun isConnected() = rsocket != null
    fun isAlive() = isConnected() && lastReceivedHeartbeat > Instant.now().minusSeconds(heartbeatTimeout)

    fun disposeRsocket() {
        rsocket?.dispose()
        rsocket = null
    }

    fun dispose() {
        disposeRsocket()
        maintenanceTask?.cancel(true)
        maintenanceTask = null
    }

    companion object {
        private const val BACKOFF_FACTOR = 2.0
    }
}
