package io.axoniq.console.framework.client

import io.axoniq.console.framework.api.ClientSettings
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Service that holds the client settings. See [ClientSettingsObserver] for more information.
 */
class ClientSettingsService {
    private val observers = CopyOnWriteArrayList<ClientSettingsObserver>()
    private var settings: ClientSettings? = null

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

    fun updateSettings(settings: ClientSettings) {
        clearSettings()
        this.settings = settings
        observers.forEach { it.onConnectedWithSettings(settings) }
    }
}