/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.api;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for managing the lifecycle of a connector.
 *
 * @author Jan Galinski
 * @since 5.2.0
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
