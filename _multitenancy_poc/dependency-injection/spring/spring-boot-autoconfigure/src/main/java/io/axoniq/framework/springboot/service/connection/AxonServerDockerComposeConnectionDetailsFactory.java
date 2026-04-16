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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.docker.compose.core.RunningService;
import org.springframework.boot.docker.compose.service.connection.DockerComposeConnectionDetailsFactory;
import org.springframework.boot.docker.compose.service.connection.DockerComposeConnectionSource;

/**
 * ConnectionDetailsFactory implementation that recognizes Spring Boot Docker Compose-managed Docker containers running
 * Axon Server and creates an AxonServerConnectionDetails bean with the details to connect to AxonServer within that
 * container.
 *
 * @author Allard Buijze
 * @since 4.9.0
 */
public class AxonServerDockerComposeConnectionDetailsFactory extends DockerComposeConnectionDetailsFactory<AxonServerConnectionDetails> {

    private static final Logger logger = LoggerFactory.getLogger(AxonServerDockerComposeConnectionDetailsFactory.class);

    /**
     * Initializes the factory to look for containers running the "docker.axoniq.io/axoniq/axonserver" image.
     */
    public AxonServerDockerComposeConnectionDetailsFactory() {
        super("axoniq/axonserver", "io.axoniq.axonserver.connector.AxonServerConnectionFactory");
    }

    @Override
    protected AxonServerConnectionDetails getDockerComposeConnectionDetails(DockerComposeConnectionSource source) {
        RunningService runningService = source.getRunningService();
        if (runningService == null) {
            return null;
        }
        int port = runningService.ports().get(8124);
        int uiPort = runningService.ports().get(8024);
        String host = runningService.host();

        //noinspection HttpUrlsUsage
        logger.info("Detected Axon Server container. To access the dashboard, visit http://{}:{}", host, uiPort);

        return () -> host + ":" + port;
    }
}
