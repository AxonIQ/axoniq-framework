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
import io.axoniq.platform.framework.api.ClientStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Service that holds the client settings. See [ClientSettingsObserver] for more information.
 */
class ClientSettingsService {
    private val observers = CopyOnWriteArrayList<ClientSettingsObserver>()
    private var clientStatus: ClientStatus = ClientStatus.PENDING
    private var settings: ClientSettingsV2? = null
    private val logger = KotlinLogging.logger { }

    fun clearSettings() {
        logger.debug { "Clearing client settings" }
        if (settings != null) {
            settings = null
            observers.forEach { it.onDisconnected() }
        }
    }

    fun subscribeToSettings(observer: ClientSettingsObserver) {
        logger.debug { "Subscribing to client settings $observer" }
        this.observers.add(observer)
        if (settings != null) {
            observer.onConnectionUpdate(clientStatus, settings!!)
        }
    }

    fun updateClientStatus(clientStatus: ClientStatus) {
        logger.debug { "Client status changed to $clientStatus" }
        this.clientStatus = clientStatus
        if (settings != null) {
            observers.forEach { it.onConnectionUpdate(clientStatus, settings!!) }
        }
    }

    fun updateSettings(settings: ClientSettingsV2) {
        clearSettings()
        logger.debug { "Client settings changed to $settings" }
        this.settings = settings
        observers.forEach { it.onConnectionUpdate(clientStatus, settings) }
    }
}