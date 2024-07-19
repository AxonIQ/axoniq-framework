/*
 * Copyright (c) 2024. AxonIQ B.V.
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
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Service that holds the client settings. See [ClientSettingsObserver] for more information.
 */
class ClientSettingsService {
    private val observers = CopyOnWriteArrayList<ClientSettingsObserver>()
    private var settings: ClientSettingsV2? = null

    fun clearSettings() {
        if(settings != null) {
            settings = null
            observers.forEach { it.onDisconnected() }
        }
    }

    fun subscribeToSettings(observer: ClientSettingsObserver) {
        this.observers.add(observer)
        if(settings != null) {
            observer.onConnectedWithSettings(settings!!)
        }
    }

    fun updateSettings(settings: ClientSettingsV2) {
        clearSettings()
        this.settings = settings
        observers.forEach { it.onConnectedWithSettings(settings) }
    }
}