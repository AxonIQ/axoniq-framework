/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.distributed.tracing;

/**
 * Settings controlling tracing for distributed messaging connectors.
 * <p>
 * Register these settings as a configuration component to enable or disable tracing for individual connector types.
 * When no settings are registered, tracing is enabled for every distributed connector.
 *
 * @param commandBusConnectorEnabled whether the {@code CommandBusConnector} is decorated with tracing
 * @param queryBusConnectorEnabled   whether the {@code QueryBusConnector} is decorated with tracing
 * @author Mateusz Nowak
 * @since 5.3.0
 */
public record DistributedTracingSettings(boolean commandBusConnectorEnabled,
                                         boolean queryBusConnectorEnabled) {

    /**
     * Returns the default settings, with every distributed connector enabled for tracing.
     *
     * @return the all-enabled default settings
     */
    public static DistributedTracingSettings enabledByDefault() {
        return new DistributedTracingSettings(true, true);
    }

    /**
     * Returns settings with command bus connector tracing set to {@code enabled}.
     *
     * @param enabled whether command bus connector tracing is enabled
     * @return settings with the updated command bus connector tracing value
     */
    public DistributedTracingSettings withCommandBusConnectorEnabled(boolean enabled) {
        return new DistributedTracingSettings(enabled, queryBusConnectorEnabled);
    }

    /**
     * Returns settings with query bus connector tracing set to {@code enabled}.
     *
     * @param enabled whether query bus connector tracing is enabled
     * @return settings with the updated query bus connector tracing value
     */
    public DistributedTracingSettings withQueryBusConnectorEnabled(boolean enabled) {
        return new DistributedTracingSettings(commandBusConnectorEnabled, enabled);
    }
}
