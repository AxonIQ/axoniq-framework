/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.connection;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.service.connection.AxonServerConnectionDetails;
import org.junit.jupiter.api.*;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SpringBootDockerComposeIT {

    private ConfigurableApplicationContext application;

    @BeforeEach
    void setUp() {
        application = SpringApplication.run(SpringBootApplication.class,
                                            "--spring.docker.compose.file=test-docker-compose.yml",
                                            "--spring.docker.compose.skip.in-tests=false",
                                            "--spring.main.web-application-type=none",
                                            "--axon.springcloud.enabled=false");
    }

    @AfterEach
    void tearDown() {
        application.stop();
    }

    @Test
    void verifyApplicationRunsAndConnectsToAxonServerDefinedInDockerComposeFile() {
        assertThat(application.isRunning()).isTrue();

        assertThat(application.getBean(AxonServerConnectionDetails.class))
                .isNotNull()
                .describedAs("Expected an AxonServerConnectionDetails bean pointing to Axon Server in Docker");

        AxonServerConnectionManager connectionFactory = application.getBean(AxonServerConnectionManager.class);
        AxonServerConnection connection = connectionFactory.getConnection();

        await().atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(connection.isConnected()).isTrue());
    }
}
