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

import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.PlatformClientAuthentication
import io.axoniq.platform.framework.api.ConsoleClientIdentifier
import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.api.notifications.Notification
import io.axoniq.platform.framework.api.notifications.NotificationLevel
import io.axoniq.platform.framework.api.notifications.NotificationList
import io.axoniq.platform.framework.AxoniqPlatformConfiguration
import io.axoniq.platform.framework.api.ClientStatus
import io.axoniq.platform.framework.api.ClientStatusUpdate
import io.axoniq.platform.framework.client.strategy.RSocketPayloadEncodingStrategy
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.rsocket.Payload
import io.rsocket.RSocket
import io.rsocket.core.RSocketConnector
import io.rsocket.metadata.WellKnownMimeType
import io.rsocket.transport.netty.client.TcpClientTransport
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
        private val properties: AxoniqPlatformConfiguration,
        private val setupPayloadCreator: SetupPayloadCreator,
        private val registrar: RSocketHandlerRegistrar,
        private val encodingStrategy: RSocketPayloadEncodingStrategy,
        private val clientSettingsService: ClientSettingsService
) {
    private val environmentId: String = properties.environmentId
    private val accessToken: String = properties.accessToken
    private val applicationName: String = properties.applicationName
    private val host: String = properties.host
    private val port: Int = properties.port
    private val secure: Boolean = properties.secure
    private val initialDelay: Long = properties.initialDelay
    private val executor: ScheduledExecutorService = properties.getReportingTaskExecutor()
    private val instanceName: String = properties.instanceName
    private val heartbeatOrchestrator = HeartbeatOrchestrator()
    private var maintenanceTask: ScheduledFuture<*>? = null
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val connectionLock = Any()

    @Volatile
    private var rsocket: RSocket? = null

    @Volatile
    private var pendingConnection: Mono<RSocket>? = null
    private var lastConnectionTry = Instant.EPOCH
    private var connectionRetryCount = 0

    @Volatile
    private var status: ClientStatus = ClientStatus.PENDING
    private var suppressConnectMessage = false

    init {
        clientSettingsService.subscribeToSettings(heartbeatOrchestrator)

        // Server can send updated settings if necessary
        registrar.registerHandlerWithPayload(Routes.Management.SETTINGS, ClientSettingsV2::class.java) {
            clientSettingsService.updateSettings(it)
        }

        // Server sends client status updates
        registrar.registerHandlerWithPayload(Routes.Management.STATUS, ClientStatusUpdate::class.java) {
            logger.debug("Received status update from Axoniq Platform. New status: {}", it.newStatus)
            status = it.newStatus
            clientSettingsService.updateClientStatus(status)
        }

        // Server can send log requests
        registrar.registerHandlerWithPayload(Routes.Management.LOG, Notification::class.java) {
            logger.logNotification(it)
        }
    }

    /**
     * Sends a report to Axoniq Platform. If reports are paused, does nothing silently.
     */
    fun sendReport(route: String, payload: Any): Mono<Unit> {
        if (!status.enabled) {
            return Mono.empty()
        }
        return sendMessage(payload, route)
    }

    /**
     * Sends a message to the platform. If the connection attempt fails, the error is propagated to the caller.
     * Do not use this method for reports, as it does not check if reports are paused. Use [sendReport] instead.
     */
    fun sendMessage(payload: Any, route: String): Mono<Unit> {
        return getOrConnectRSocket()
                .flatMap { socket ->
                    socket.requestResponse(encodingStrategy.encode(payload, createRoutingMetadata(route)))
                            .map {
                                val notifications = encodingStrategy.decode(it, NotificationList::class.java)
                                logger.log(notifications)
                            }
                }
    }

    fun <R> retrieve(payload: Any, route: String, responseType: Class<R>): Mono<R> {
        return getOrConnectRSocket()
                .flatMap { socket ->
                    socket.requestResponse(encodingStrategy.encode(payload, createRoutingMetadata(route)))
                            .map { responsePayload ->
                                encodingStrategy.decode(responsePayload, responseType)
                            }
                }
    }

    /**
     * Returns the current RSocket connection, or attempts to establish one if not yet connected.
     * Concurrent callers share the same in-flight [pendingConnection] Mono so that only one TCP
     * connection is ever established at a time. Propagates any connection error to the caller.
     */
    private fun getOrConnectRSocket(): Mono<RSocket> {
        val current = rsocket
        if (current != null) return Mono.just(current)
        return synchronized(connectionLock) {
            pendingConnection ?: buildConnectionMono().also { pendingConnection = it }
        }
    }

    /**
     * Builds a cold-then-cached Mono that creates the RSocket connection and retrieves initial settings.
     * The Mono is cached so all concurrent subscribers share a single connection attempt.
     * [pendingConnection] is cleared (via [doOnTerminate]) once the attempt completes or fails,
     * allowing a fresh attempt on the next call to [getOrConnectRSocket].
     */
    private fun buildConnectionMono(): Mono<RSocket> {
        return createRSocketMono()
                .flatMap { socket ->
                    rsocket = socket
                    retrieveSettings().map { settings ->
                        clientSettingsService.updateSettings(settings)
                        if (!suppressConnectMessage) {
                            logger.info("Connection to Axoniq Platform set up successfully! This instance's name: $instanceName, settings: $settings")
                            suppressConnectMessage = true
                        }
                        connectionRetryCount = 0
                        socket
                    }
                }
                .doOnError { disposeCurrentConnection() }
                .doFinally { synchronized(connectionLock) { pendingConnection = null } }
                .cache()
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
        ensureConnected()
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
            logger.debug("Connecting to Axoniq Platform...")
            connectSafely()
        }
    }

    private fun connectSafely() {
        val retryCount = connectionRetryCount
        getOrConnectRSocket().subscribe(
                { /* success — logged inside buildConnectionMono */ },
                { e ->
                    if (retryCount == 2) {
                        if (suppressConnectMessage) {
                            logger.warn("Lost connection to Axoniq Platform. Will keep trying to reconnect...")
                        } else {
                            logger.warn("Unable to connect to Axoniq Platform. Will keep trying to reconnect...")
                        }
                        suppressConnectMessage = false
                    } else if (retryCount > 2 && retryCount % 10 == 0) {
                        logger.error("Still unable to reconnect to Axoniq Platform after $retryCount attempts. Reason: ${e.message}")
                    }
                    logger.debug("Failed to connect to Axoniq Platform", e)
                }
        )
    }

    private fun createRSocketMono(): Mono<RSocket> {
        val authentication = PlatformClientAuthentication(
                identification = ConsoleClientIdentifier(
                        environmentId = environmentId,
                        applicationName = applicationName,
                        nodeName = instanceName
                ),
                accessToken = accessToken
        )

        val setupPayload = encodingStrategy.encode(
                setupPayloadCreator.createReport(),
                createSetupMetadata(authentication)
        )
        return RSocketConnector.create()
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.string)
                .dataMimeType(encodingStrategy.getMimeType().string)
                .setupPayload(setupPayload)
                .acceptor { _, rsocket ->
                    Mono.just(registrar.createRespondingRSocketFor(rsocket))
                }
                .connect(tcpClientTransport())
    }

    private fun createRoutingMetadata(route: String): CompositeByteBuf {
        val metadata: CompositeByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer()
        metadata.addRouteMetadata(route)
        return metadata
    }

    private fun createSetupMetadata(auth: PlatformClientAuthentication): CompositeByteBuf {
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
            client.secure()
        } else client
    }

    fun isConnected() = rsocket != null

    fun disposeCurrentConnection() {
        val currentRSocket = rsocket
        if (currentRSocket != null) {
            rsocket = null
            currentRSocket.dispose()
            clientSettingsService.clearSettings()
        }
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
                logger.debug("Received heartbeat from Axoniq Platform. Last one was: {}", lastReceivedHeartbeat)
                lastReceivedHeartbeat = Instant.now()
                lastReceivedHeartbeat
            }
        }

        override fun onConnectionUpdate(clientStatus: ClientStatus, settings: ClientSettingsV2) {
            lastReceivedHeartbeat = Instant.now()
            this.heartbeatSendTask = executor.scheduleWithFixedDelay(
                    { sendHeartbeat().subscribe({}, { e -> logger.debug("Heartbeat failed", e) }) },
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
            this.heartbeatSendTask?.cancel(true)
            this.heartbeatSendTask = null
            this.heartbeatCheckTask?.cancel(true)
            this.heartbeatCheckTask = null
        }

        private fun checkHeartbeats(heartbeatTimeout: Long) {
            if (lastReceivedHeartbeat < Instant.now().minusMillis(heartbeatTimeout)) {
                logger.debug("Haven't received a heartbeat for {} seconds from Axoniq Platform. Reconnecting...", ChronoUnit.SECONDS.between(lastReceivedHeartbeat, Instant.now()))
                disposeCurrentConnection()
            }
        }

        private fun sendHeartbeat(): Mono<Payload> {
            return rsocket
                    ?.requestResponse(encodingStrategy.encode("", createRoutingMetadata(Routes.Management.HEARTBEAT)))
                    ?.doOnSuccess {
                        logger.debug("Heartbeat successfully sent to Axoniq Platform")
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
                .onErrorMap {
                    if (it.message?.contains("Access Denied") == true) {
                        IllegalStateException("Was unable to connect to Axoniq Platform due to invalid authentication! Make sure the access token is correct.")
                    } else {
                        it
                    }
                }
                .doOnError {
                    logger.error(it.message)
                }
    }

    private fun Logger.log(notificationList: NotificationList) {
        notificationList.messages.forEach {
            logNotification(it)
        }
    }

    private fun Logger.logNotification(it: Notification) {
        val text = it.message
        when (it.level) {
            NotificationLevel.Debug -> this.debug(text)
            NotificationLevel.Info -> this.info(text)
            NotificationLevel.Warn -> this.warn(text)
        }
    }
}
