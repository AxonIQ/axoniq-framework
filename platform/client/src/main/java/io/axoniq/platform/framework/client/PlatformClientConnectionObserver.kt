/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.platform.framework.client

import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.ClientStatus

/**
 * Observes the established connection and the settings provided by the server.
 * The [onDisconnected] method is called when the connection is lost, or just before new settings
 * are being updated to provide cleanup. The [onConnected] method is called when the connection is
 * established or the settings are updated
 */
interface PlatformClientConnectionObserver {
    /**
     * Called when the connection is established, the settings are updated, or the client's status changes.
     * @param settings the settings provided by the server
     */
    fun onConnected(clientStatus: ClientStatus, settings: ClientSettingsV2)

    /**
     * Called when the connection is lost, or just before new settings are being updated to provide cleanup.
     */
    fun onDisconnected()

    /**
     * Called when the connection can not be established. Will be called every time the client tries to connect, but fails. The reason for the failure is provided as a parameter.
     *
     * @param reason The reason for being unreachable.
     */
    fun onUnreachable(reason: UnreachableReason) {
        // Default implementation does nothing, as not all observers need to react to this event
    }

    enum class UnreachableReason {
        INVALID_AUTHENTICATION,
        NO_CONNECTION,
        OTHER,
    }
}