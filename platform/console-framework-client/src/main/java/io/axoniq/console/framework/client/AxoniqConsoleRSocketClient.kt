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

import io.axoniq.console.framework.api.ClientSettings
import io.axoniq.console.framework.api.ClientSettingsV2
import io.axoniq.console.framework.api.Routes
import io.axoniq.console.framework.api.notifications.NotificationLevel
import io.axoniq.console.framework.api.notifications.NotificationList
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
import org.slf4j.Logger
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
 * Establishing a connection works as follows:
 * - The client will send a setup payload to the server, containing the authentication information
 * - The client will retrieve the settings from the server, and update the [ClientSettingsService] with the new settings
 * - The client will start sending heartbeats to the server, and will check if it receives heartbeats from the server
 *
 * The server is in control of these settings. Of course, the user can manipulate these as well themselves.
 * The server should be resilient against this manipulation in the form of rate limiting.
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
        private val clientSettingsService: ClientSettingsService,
        private val executor: ScheduledExecutorService,
        private val nodeName: String,
) : Lifecycle {
    private val heartbeatOrchestrator = HeartbeatOrchestrator()
    private var maintenanceTask: ScheduledFuture<*>? = null
    private val logger = LoggerFactory.getLogger(this::class.java)

    private var rsocket: RSocket? = null
    private var lastConnectionTry = Instant.EPOCH
    private var connectionRetryCount = 0

    init {
        clientSettingsService.subscribeToSettings(heartbeatOrchestrator)

        // Server can send updated settings if necessary
        registrar.registerHandlerWithPayload(Routes.Management.SETTINGS, ClientSettingsV2::class.java) {
            clientSettingsService.updateSettings(it)
        }
    }

    override fun registerLifecycleHandlers(registry: Lifecycle.LifecycleRegistry) {
        registry.onStart(Phase.EXTERNAL_CONNECTIONS, this::start)
        registry.onShutdown(Phase.EXTERNAL_CONNECTIONS, this::disposeClient)
    }

    /**
     * Sends a message to the AxonIQ Console. If there is no connection active, does nothing silently.
     * The connection will automatically be setup. Losing a few reports is no problem.
     */
    fun send(route: String, payload: Any): Mono<Unit> {
        return rsocket
                ?.requestResponse(encodingStrategy.encode(payload, createRoutingMetadata(route)))
                ?.map {
                    val notifications = encodingStrategy.decode(it, NotificationList::class.java)
                    logger.log(notifications)
                }
                ?: Mono.empty()
    }

    /**
     * Starts the connection, and starts the maintenance task.
     * The task will ensure that if heartbeats are missed the connection is killed, as well as re-setup in case
     * the connection was lost. The task will do so with an exponential backoff with factor 2, up to a maximum of
     * 60 seconds.
     */
    fun start() {
        if (this.maintenanceTask != null) {
            return
        }
        this.maintenanceTask = executor.scheduleWithFixedDelay(
                this::ensureConnected,
                initialDelay,
                1000, TimeUnit.MILLISECONDS
        )
    }

    private fun ensureConnected() {
        if (!isConnected()) {
            val secondsToWaitForReconnect = BACKOFF_FACTOR.pow(connectionRetryCount.toDouble()).coerceAtMost(60.0)
            if (ChronoUnit.SECONDS.between(lastConnectionTry, Instant.now()) < secondsToWaitForReconnect) {
                return
            }
            connectionRetryCount += 1
            lastConnectionTry = Instant.now()
            logger.info("Connecting to AxonIQ Console...")
            connectSafely()
        }
    }

    private fun connectSafely() {
        try {
            rsocket = createRSocket()
            // Fetch the client settings from the server
            val settings = retrieveSettings().block()
                    ?: throw IllegalStateException("Could not receive the settings from AxonIQ console!")
            clientSettingsService.updateSettings(settings)
            logger.info("Connection to AxonIQ Console set up successfully! Settings: $settings")
            connectionRetryCount = 0
        } catch (e: Exception) {
            disposeCurrentConnection()
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
                    disposeCurrentConnection()
                }
        return if (secure) {
            return client.secure()
        } else client
    }

    fun isConnected() = rsocket != null

    fun disposeCurrentConnection() {
        rsocket?.dispose()
        rsocket = null
        clientSettingsService.clearSettings()
    }

    fun disposeClient() {
        disposeCurrentConnection()
        maintenanceTask?.cancel(true)
        maintenanceTask = null
    }

    companion object {
        private const val BACKOFF_FACTOR = 2.0
    }

    private inner class HeartbeatOrchestrator : ClientSettingsObserver {
        private var heartbeatSendTask: ScheduledFuture<*>? = null
        private var heartbeatCheckTask: ScheduledFuture<*>? = null
        private var lastReceivedHeartbeat = Instant.now()

        init {
            registrar.registerHandlerWithoutPayload(Routes.Management.HEARTBEAT) {
                logger.debug("Received heartbeat from AxonIQ Console. Last one was: {}", lastReceivedHeartbeat)
                lastReceivedHeartbeat = Instant.now()
                lastReceivedHeartbeat
            }
        }

        override fun onConnectedWithSettings(settings: ClientSettingsV2) {
            lastReceivedHeartbeat = Instant.now()
            this.heartbeatSendTask = executor.scheduleWithFixedDelay(
                    { sendHeartbeat().subscribe() },
                    0,
                    settings.heartbeatInterval,
                    TimeUnit.MILLISECONDS
            )

            this.heartbeatCheckTask = executor.scheduleWithFixedDelay(
                    { checkHeartbeats(settings.heartbeatTimeout) },
                    0,
                    1000,
                    TimeUnit.MILLISECONDS
            )

        }

        override fun onDisconnected() {
            logger.info("Disconnected, stopping heartbeat tasks")
            this.heartbeatSendTask?.cancel(true)
            this.heartbeatCheckTask?.cancel(true)
        }


        private fun checkHeartbeats(heartbeatTimeout: Long) {
            if (lastReceivedHeartbeat < Instant.now().minusMillis(heartbeatTimeout)) {
                logger.info("Haven't received a heartbeat for {} seconds from AxonIQ Console. Reconnecting...", ChronoUnit.SECONDS.between(lastReceivedHeartbeat, Instant.now()))
                disposeCurrentConnection()
            }
        }

        private fun sendHeartbeat(): Mono<Payload> {
            return rsocket
                    ?.requestResponse(encodingStrategy.encode("", createRoutingMetadata(Routes.Management.HEARTBEAT)))
                    ?.doOnSuccess {
                        logger.debug("Heartbeat successfully sent to AxonIQ Console")
                    }
                    ?: Mono.empty()
        }
    }

    private fun retrieveSettings(): Mono<ClientSettingsV2> {
        return rsocket!!
                .requestResponse(encodingStrategy.encode("", createRoutingMetadata(Routes.Management.SETTINGS_V2)))
                .map {
                    encodingStrategy.decode(it, ClientSettingsV2::class.java)
                }
                .doOnError {
                    if (it.message?.contains("Access Denied") == true) {
                        logger.info("Was unable to send call to AxonIQ Console since authentication was incorrect!")
                    }
                }
    }

    private fun Logger.log(notificationList: NotificationList) {
        notificationList.messages.forEach {
            when (it.level) {
                NotificationLevel.Debug -> this.debug(it.message)
                NotificationLevel.Info -> this.info(it.message)
                NotificationLevel.Warn -> this.warn(it.message)
            }
        }
    }
}
