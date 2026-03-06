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

import io.axoniq.license.entitlement.PlatformConnector
import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.ClientStatus
import io.axoniq.platform.framework.api.Routes
import io.axoniq.platform.framework.client.AxoniqConsoleRSocketClient
import io.axoniq.platform.framework.client.ClientSettingsObserver
import io.axoniq.platform.framework.client.ClientSettingsService
import io.axoniq.platform.framework.client.RSocketHandlerRegistrar
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * RSocket-based implementation of [PlatformConnector] that bridges the Axoniq Platform
 * transport layer to the entitlement manager's callback interface.
 *
 * This adapter handles:
 * - Subscribing to connection lifecycle events via [ClientSettingsService]
 * - Registering a handler for pushed license updates via [RSocketHandlerRegistrar]
 * - Retrieving licenses via [AxoniqConsoleRSocketClient] request-response
 *
 * @author Stefan Mirkovic
 */
class RSocketPlatformConnector(
    private val rSocket: AxoniqConsoleRSocketClient,
    private val registrar: RSocketHandlerRegistrar,
    private val clientSettingsService: ClientSettingsService,
) : PlatformConnector {

    private val currentStatus: AtomicReference<ClientStatus> = AtomicReference(ClientStatus.PENDING)

    override fun initialize(callback: PlatformConnector.Callback) {
        clientSettingsService.subscribeToSettings(object : ClientSettingsObserver {
            override fun onConnectionUpdate(clientStatus: ClientStatus, settings: ClientSettingsV2) {
                if (clientStatus == ClientStatus.BLOCKED) {
                    currentStatus.set(clientStatus)
                    callback.onBlocked()
                } else {
                    val wasEnabled = currentStatus.get().enabled
                    currentStatus.set(clientStatus)
                    if (!wasEnabled && clientStatus.enabled) {
                        callback.onEnabled()
                    }
                }
            }

            override fun onDisconnected() {
                currentStatus.set(ClientStatus.PENDING)
                callback.onDisconnected()
            }
        })

        registrar.registerHandlerWithPayload(Routes.License.LICENSE, String::class.java) { payload ->
            callback.onLicensePushed(payload)
            true
        }
    }

    override fun retrieveLicense(onSuccess: Consumer<String>, onError: Consumer<Throwable>) {
        rSocket.retrieve("", Routes.License.LICENSE, String::class.java)
            .subscribeOn(Schedulers.boundedElastic())
            .publishOn(Schedulers.boundedElastic())
            .doOnNext { onSuccess.accept(it) }
            .doOnError { onError.accept(it) }
            .onErrorResume { Mono.empty() }
            .subscribe()
    }
}
