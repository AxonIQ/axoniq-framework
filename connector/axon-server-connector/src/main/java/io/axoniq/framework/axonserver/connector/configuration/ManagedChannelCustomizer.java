/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.configuration;

import io.grpc.ManagedChannelBuilder;

import java.util.function.UnaryOperator;

/**
 * Customizer to add more customizations to a managed channel to Axon Server.
 *
 * @author Marc Gathier
 * @since 4.4.3
 */
@FunctionalInterface
public interface ManagedChannelCustomizer extends UnaryOperator<ManagedChannelBuilder<?>> {

    /**
     * Returns a no-op {@link ManagedChannelCustomizer}.
     *
     * @return a no-op {@link ManagedChannelCustomizer}
     */
    static ManagedChannelCustomizer identity() {
        return c -> c;
    }
}
