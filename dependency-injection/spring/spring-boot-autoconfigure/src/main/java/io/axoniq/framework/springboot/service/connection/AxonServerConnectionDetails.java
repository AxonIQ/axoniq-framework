/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.service.connection;

import org.springframework.boot.autoconfigure.service.connection.ConnectionDetails;

/**
 * ConnectionDetails implementation carrying the connection details for an Axon Server instance.
 * <p>
 * Note that this is not a replacement for full connectivity configuration. ConnectionDetails are designed to only carry
 * the endpoint at which a node runs.
 *
 * @author Allard Buijze
 * @since 4.9.0
 */
public interface AxonServerConnectionDetails extends ConnectionDetails {

    /**
     * The addresses of the routing servers to use to connect to an Axon Server cluster. The string should contain a
     * comma separated list of AxonServer servers. Each element is hostname or hostname:grpcPort. When no grpcPort is
     * specified, default port 8124 is used.
     *
     * @return a string containing the addresses of the routing servers to connect with
     */
    String routingServers();
}
