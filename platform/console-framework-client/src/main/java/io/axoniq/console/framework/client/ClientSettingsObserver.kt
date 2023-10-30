package io.axoniq.console.framework.client

import io.axoniq.console.framework.api.ClientSettings

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
    fun onConnectedWithSettings(settings: ClientSettings)

    /**
     * Called when the connection is lost, or just before new settings are being updated to provide cleanup.
     */
    fun onDisconnected()
}