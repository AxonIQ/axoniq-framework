/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.api;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for managing the lifecycle of a connector.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public interface ConnectorLifecycle {

    /**
     * Starts the connector.
     */
    void start();

    /**
     * Shuts down the connector gracefully.
     *
     * @return a {@link CompletableFuture} that completes when the connector has been shutdown.
     */
    CompletableFuture<Void> shutdownDispatching();

    /**
     * Disconnects the connector.
     *
     * @return a {@link CompletableFuture} that completes when the connector has been disconnected.
     */
    CompletableFuture<Void> disconnect();
}
