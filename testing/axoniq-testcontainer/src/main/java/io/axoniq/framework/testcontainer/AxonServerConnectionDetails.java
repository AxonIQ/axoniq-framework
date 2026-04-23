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

package io.axoniq.framework.testcontainer;

import org.springframework.boot.autoconfigure.service.connection.ConnectionDetails;

/**
 * {@link ConnectionDetails} for an Axon Server instance.
 * <p>
 * Provides the gRPC routing address needed to connect to Axon Server. Intended to be produced by a
 * {@link org.springframework.boot.testcontainers.service.connection.ContainerConnectionDetailsFactory}
 * when {@link AxonServerContainer} is annotated with
 * {@link org.springframework.boot.testcontainers.service.connection.ServiceConnection}.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see AxonServerContainerConnectionDetailsFactory
 */
public interface AxonServerConnectionDetails extends ConnectionDetails {

    /**
     * the gRPC address of the Axon Server instance in {@code host:port} format
     *
     * @return the routing servers address
     */
    String routingServers();
}
