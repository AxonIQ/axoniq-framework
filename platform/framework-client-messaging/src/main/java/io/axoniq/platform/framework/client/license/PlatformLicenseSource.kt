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
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Instant
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

class PlatformLicenseSource(
        private val rSocket: AxoniqConsoleRSocketClient,
        private val registrar: RSocketHandlerRegistrar,
        private val clientSettingsService: ClientSettingsService,
        private val executorService: ScheduledExecutorService,
) : LicenseSource, ClientSettingsObserver {

    private var listener: LicenseSource.Listener? = null
    private var currentLicense: String? = null
    private var subscription: Disposable? = null
    private val currentStatus: AtomicReference<ClientStatus> = AtomicReference(ClientStatus.PENDING)
    private var lastLicenseReceivedTime: Instant? = null

    init {
        clientSettingsService.subscribeToSettings(this)

        registrar.registerHandlerWithPayload(Routes.License.LICENSE, String::class.java) { payload ->
            logger.debug { "Received license update via request-response: $payload" }
            if (currentStatus.get().enabled) {
                currentLicense = payload
                lastLicenseReceivedTime = Instant.now()
                listener?.onLicenseAvailable(payload)
            }
            true
        }
    }

    override fun isConfigured(): Boolean {
        return true
    }

    override fun priority(): Int {
        return 70
    }

    override fun register(newListener: LicenseSource.Listener) {
        check(listener == null) { "Already has a listener" }
        this.listener = newListener
        currentLicense?.let { this.listener!!.onLicenseAvailable(it) }
    }

    override fun onConnectionUpdate(clientStatus: ClientStatus, settings: ClientSettingsV2) {
        logger.debug { "Connection update: $clientStatus" }

        if (clientStatus == ClientStatus.BLOCKED) {
            this.currentLicense = null
            this.lastLicenseReceivedTime = null
            subscription?.dispose()
            subscription = null
            this.listener?.onLicenseUnavailable(LicenseSource.LicenseUnavailableReason.LICENSE_APPLICATION_LIMIT_EXCEEDED)
        } else
            if (!this.currentStatus.get().enabled && clientStatus.enabled) {
                retrieveLicense()
            }
        currentStatus.set(clientStatus)
    }

    override fun onDisconnected() {
        logger.debug { "Disconnected" }
        currentStatus.set(ClientStatus.PENDING)

        // Schedule a delayed check: if still not reconnected after DISCONNECT_GRACE_MS,
        // notify the entitlement system that the license source is unreachable.
        // This delay prevents false alarms during brief reconnects.
        executorService.schedule({
            if (!currentStatus.get().enabled) {
                if (currentLicense != null) {
                    logger.warn { "Platform disconnected and did not reconnect within grace window. Marking license source as unreachable." }
                } else {
                    logger.warn { "Platform not reachable. Marking license source as unreachable." }
                }
                listener?.onLicenseSourceUnreachable()
            }
        }, DISCONNECT_GRACE_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
    }

    private fun retrieveLicense(attempt: Int = 1) {
        rSocket.retrieve("", Routes.License.LICENSE, String::class.java)
                .subscribeOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.boundedElastic())
                .doOnNext { response ->
                    logger.debug { "Received license via request-response" }
                    currentLicense = response
                    lastLicenseReceivedTime = Instant.now()
                    listener?.onLicenseAvailable(response)
                }
                .doOnError { error ->
                    logger.debug { "Failed to retrieve license via request-response: ${error.message}" }
                    if (attempt == 3) {
                        logger.warn { "Was unable to retrieve License from Axoniq Platform" }
                        listener?.onLicenseSourceUnreachable()
                    }

                    executorService.schedule({
                        retrieveLicense(attempt + 1)
                    }, RETRY_INTERVAL_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
                }
                .onErrorResume { Mono.empty() }
                .subscribe()
    }

    companion object {
        private val logger = KotlinLogging.logger { }
        private const val RETRY_INTERVAL_MS = 30_000L // 30 seconds
        private const val DISCONNECT_GRACE_MS = 60_000L // 1 minute before notifying entitlement system
    }
}
