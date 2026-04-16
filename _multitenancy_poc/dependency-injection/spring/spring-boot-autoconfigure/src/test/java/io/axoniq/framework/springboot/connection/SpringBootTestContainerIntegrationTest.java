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

package io.axoniq.framework.springboot.connection;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.service.connection.AxonServerConnectionDetails;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
class SpringBootTestContainerIntegrationTest {

    @Container
    @ServiceConnection
    private final static AxonServerContainer axonServer = new AxonServerContainer().withDevMode(true);

    @Autowired
    private AxonServerConfiguration axonServerConfiguration;

    @Autowired
    private AxonServerConnectionDetails connectionDetails;

    @Autowired
    private AxonServerConnectionManager axonServerConnectionManager;

    @Test
    void verifyApplicationStartsNormallyWithAxonServerInstance() {
        assertTrue(axonServer.isRunning());
        assertNotNull(connectionDetails);
        assertEquals(axonServer.getHost() + ":" + axonServer.getGrpcPort(), connectionDetails.routingServers());
        assertNotNull(axonServerConfiguration);

        assertNotEquals("localhost:8024", axonServerConfiguration.getServers());

        AxonServerConnection connection = axonServerConnectionManager.getConnection();

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertTrue(connection.isConnected()));
    }
}