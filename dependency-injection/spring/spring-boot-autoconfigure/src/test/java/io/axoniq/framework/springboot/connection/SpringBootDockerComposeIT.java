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
                                            "--spring.docker.compose.skip.in-tests=false");
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
