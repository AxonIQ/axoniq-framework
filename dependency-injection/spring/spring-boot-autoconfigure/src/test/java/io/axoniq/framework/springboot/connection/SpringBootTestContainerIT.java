/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.connection;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.service.connection.AxonServerConnectionDetails;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "axon.springcloud.enabled=false",
        "axon.workflow.enabled=false"
})
class SpringBootTestContainerIT {

    // No @Container: that annotation's afterAll stop() call isn't skipped for reusable containers, which would
    // tear down the build-wide shared container out from under other consumers.
    private static final AxonServerContainer axonServer = SharedAxonServerContainer.INSTANCE;

    @BeforeAll
    static void startSharedContainer() {
        SharedAxonServerContainer.ensureStarted();
    }

    @Autowired
    private AxonServerConfiguration axonServerConfiguration;

    @Autowired
    private AxonServerConnectionDetails connectionDetails;

    @Autowired
    private AxonServerConnectionManager axonServerConnectionManager;

    @Test
    void verifyApplicationStartsNormallyWithAxonServerInstance() {
        assertThat(axonServer.isRunning()).isTrue();
        assertThat(connectionDetails).isNotNull();
        assertThat(connectionDetails.routingServers()).isEqualTo(axonServer.getHost() + ":" + axonServer.getGrpcPort());
        assertThat(axonServerConfiguration).isNotNull();

        assertThat(axonServerConfiguration.getServers()).isNotEqualTo("localhost:8024");

        AxonServerConnection connection = axonServerConnectionManager.getConnection();

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(connection.isConnected()).isTrue());
    }

    @TestConfiguration
    static class SharedContainerConnectionDetailsConfig {

        @Bean
        AxonServerConnectionDetails axonServerConnectionDetails() {
            return () -> axonServer.getHost() + ":" + axonServer.getGrpcPort();
        }
    }
}
