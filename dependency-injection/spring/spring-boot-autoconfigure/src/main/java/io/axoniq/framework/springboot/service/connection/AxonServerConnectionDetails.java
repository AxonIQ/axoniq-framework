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
