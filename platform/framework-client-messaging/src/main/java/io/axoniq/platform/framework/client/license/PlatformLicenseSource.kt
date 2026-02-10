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
package io.axoniq.platform.framework.client.license

import io.axoniq.license.entitlement.LicenseSource
import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.ClientStatus
import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.client.AxoniqConsoleRSocketClient
import io.axoniq.platform.framework.client.ClientSettingsObserver
import io.axoniq.platform.framework.client.ClientSettingsService
import io.axoniq.platform.framework.client.RSocketHandlerRegistrar
import io.github.oshai.kotlinlogging.KotlinLogging
import reactor.core.Disposable
import reactor.core.scheduler.Schedulers
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class PlatformLicenseSource(
        private val rSocket: AxoniqConsoleRSocketClient,
        private val registrar: RSocketHandlerRegistrar,
        private val clientSettingsService: ClientSettingsService
) : LicenseSource, ClientSettingsObserver {

    private var listener: LicenseSource.Listener? = null
    private var currentLicense: String? = null
    private var subscription: Disposable? = null
    private val currentStatus: AtomicReference<ClientStatus> = AtomicReference(ClientStatus.PENDING)
    private var lastLicenseReceivedTime: Instant? = null
    private var reconnectionThread: Thread? = null
    private var shutdown = false

    init {
        clientSettingsService.subscribeToSettings(this)

        // Start virtual thread for periodic reconnection
        reconnectionThread = Thread.ofVirtual().start {
            reconnectionLoop()
        }
    }

    override fun isConfigured(): Boolean {
        return true
    }

    override fun priority(): Int {
        return 70
    }

    override fun register(newListener: LicenseSource.Listener?) {
        check(listener == null) { "Already has a listener" }
        this.listener = newListener
        if (currentLicense != null) {
            this.listener!!.onLicenseAvailable(currentLicense)
        }
    }

    override fun onConnectionUpdate(clientStatus: ClientStatus, settings: ClientSettingsV2) {
        logger.debug { "Connection update: $clientStatus" }
        currentStatus.set(clientStatus)

        if(clientStatus == ClientStatus.BLOCKED) {
            this.currentLicense = null
            this.lastLicenseReceivedTime = null
            subscription?.dispose()
            subscription = null
            if (this.listener != null) {
                this.listener!!.onLicenseUnavailable()
            }
        } else if(clientStatus != ClientStatus.PENDING) {
            // Immediately establish stream when connection is available
            logger.debug { "Connection enabled, establishing license stream immediately" }
            establishLicenseStream()
        }
    }

    override fun onDisconnected() {
        logger.debug { "Disconnected" }
        currentStatus.set(ClientStatus.PENDING)
        subscription?.dispose()
        subscription = null
    }

    private fun reconnectionLoop() {
        logger.debug { "Starting license reconnection loop" }
        while (!shutdown) {
            try {
                val status = currentStatus.get()
                val sub = subscription

                // Check if we need to establish/re-establish the stream
                if (status != ClientStatus.BLOCKED && status != ClientStatus.PENDING &&
                    (sub == null || sub.isDisposed)) {
                    logger.debug { "Establishing license stream (status: $status)" }
                    establishLicenseStream()
                }

                // Check if we've exceeded the grace period without a license
                val lastReceived = lastLicenseReceivedTime
                if (currentLicense != null && lastReceived != null) {
                    val timeSinceLastLicense = java.time.Duration.between(lastReceived, Instant.now()).toMillis()
                    if (timeSinceLastLicense > GRACE_PERIOD_MS) {
                        logger.warn { "Grace period exceeded without license update, marking as unreachable" }
                        currentLicense = null
                        lastLicenseReceivedTime = null
                        listener?.onLicenseSourceUnreachable()
                    }
                }

                Thread.sleep(RETRY_INTERVAL_MS)
            } catch (e: InterruptedException) {
                logger.debug { "Reconnection loop interrupted" }
                break
            } catch (e: Exception) {
                logger.warn { "Error in reconnection loop: ${e.message}" }
            }
        }
        logger.info { "License reconnection loop stopped" }
    }

    private fun establishLicenseStream() {
        // Dispose old subscription safely
        try {
            subscription?.dispose()
        } catch (e: Exception) {
            logger.debug { "Error disposing old subscription: ${e.message}" }
        }

        subscription = rSocket.streamForUpdates("", Routes.License.LICENSE, String::class.java)
                .onErrorResume { error ->
                    logger.debug { "License stream error, will retry: ${error.message}" }
                    reactor.core.publisher.Flux.empty()
                }
                .subscribeOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.boundedElastic())
                .doOnNext { response ->
                    logger.debug { "Received license from stream: $response" }
                    currentLicense = response
                    lastLicenseReceivedTime = Instant.now()
                    listener?.onLicenseAvailable(response)
                }
                .doOnComplete {
                    logger.debug { "License stream completed" }
                }
                .doFinally {
                    logger.debug { "License stream finalized" }
                    subscription = null
                }
                .subscribe(
                    {},  // onNext already handled by doOnNext
                    { error ->
                        // This should rarely be called due to onErrorResume, but just in case
                        logger.debug { "Unexpected license stream error: ${error.message}" }
                    }
                )
    }

    fun shutdown() {
        shutdown = true
        reconnectionThread?.interrupt()
        subscription?.dispose()
    }

    companion object {
        private val logger = KotlinLogging.logger { }
        private const val RETRY_INTERVAL_MS = 30_000L // 30 seconds
        private const val GRACE_PERIOD_MS = 5 * 60 * 1000L // 5 minutes
    }
}
