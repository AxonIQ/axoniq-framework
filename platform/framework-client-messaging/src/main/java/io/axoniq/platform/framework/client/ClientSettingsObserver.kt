/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
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

/**
 * Observes the established connection and the settings provided by the server.
 * The [onDisconnected] method is called when the connection is lost, or just before new settings
 * are being updated to provide cleanup. The [onConnectedWithSettings] method is called when the connection is
 * established or the settings are updated
 */
interface ClientSettingsObserver {
    /**
     * Called when the connection is established or the settings are updated.
     * @param settings the settings provided by the server
     */
    fun onConnectedWithSettings(settings: ClientSettingsV2)

    /**
     * Called when the connection is lost, or just before new settings are being updated to provide cleanup.
     */
    fun onDisconnected()
}