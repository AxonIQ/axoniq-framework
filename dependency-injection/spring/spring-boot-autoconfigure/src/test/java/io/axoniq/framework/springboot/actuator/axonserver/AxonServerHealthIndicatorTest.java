/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.actuator.axonserver;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.actuator.HealthStatus;
import org.junit.jupiter.api.*;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AxonServerHealthIndicator}.
 *
 * @author Steven van Beelen
 */
class AxonServerHealthIndicatorTest {

    private AxonServerConnectionManager connectionManager;

    private AxonServerHealthIndicator testSubject;

    @BeforeEach
    void setUp() {
        connectionManager = mock(AxonServerConnectionManager.class);
        testSubject = new AxonServerHealthIndicator(connectionManager);
    }

    @Test
    void doHealthCheckStatusReturnsUp() {
        String testContextOne = "context-one";
        String testContextTwo = "context-two";
        String expectedDetailsContextOne = testContextOne + ".connection.active";
        String expectedDetailsContextTwo = testContextTwo + ".connection.active";

        Map<String, Boolean> testConnections = new HashMap<>();
        testConnections.put(testContextOne, true);
        testConnections.put(testContextTwo, true);
        when(connectionManager.connections()).thenReturn(testConnections);

        Health.Builder healthBuilder = new Health.Builder();

        testSubject.doHealthCheck(healthBuilder);

        Health result = healthBuilder.build();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.UP.getCode());

        Map<String, Object> resultDetails = result.getDetails();
        assertThat(resultDetails).isNotEmpty();
        assertThat(resultDetails.size()).isEqualTo(2);
        String detailsContextOne = (String) resultDetails.get(expectedDetailsContextOne);
        assertThat(detailsContextOne).isNotNull();
        assertThat(detailsContextOne).isEqualTo(Status.UP.getCode());
        String detailsContextTwo = (String) resultDetails.get(expectedDetailsContextTwo);
        assertThat(detailsContextTwo).isNotNull();
        assertThat(detailsContextTwo).isEqualTo(Status.UP.getCode());
    }

    @Test
    void doHealthCheckStatusReturnsWarning() {
        String testContextOne = "context-one";
        String testContextTwo = "context-two";
        String expectedDetailsContextOne = testContextOne + ".connection.active";
        String expectedDetailsContextTwo = testContextTwo + ".connection.active";

        Map<String, Boolean> testConnections = new HashMap<>();
        testConnections.put(testContextOne, true);
        testConnections.put(testContextTwo, false);
        when(connectionManager.connections()).thenReturn(testConnections);

        Health.Builder healthBuilder = new Health.Builder();

        testSubject.doHealthCheck(healthBuilder);

        Health result = healthBuilder.build();
        assertThat(result.getStatus()).isEqualTo(HealthStatus.WARN);

        Map<String, Object> resultDetails = result.getDetails();
        assertThat(resultDetails).isNotEmpty();
        assertThat(resultDetails.size()).isEqualTo(2);
        String detailsContextOne = (String) resultDetails.get(expectedDetailsContextOne);
        assertThat(detailsContextOne).isNotNull();
        assertThat(detailsContextOne).isEqualTo(Status.UP.getCode());
        String detailsContextTwo = (String) resultDetails.get(expectedDetailsContextTwo);
        assertThat(detailsContextTwo).isNotNull();
        assertThat(detailsContextTwo).isEqualTo(Status.DOWN.getCode());
    }

    @Test
    void doHealthCheckStatusReturnsDown() {
        String testContextOne = "context-one";
        String testContextTwo = "context-two";
        String expectedDetailsContextOne = testContextOne + ".connection.active";
        String expectedDetailsContextTwo = testContextTwo + ".connection.active";

        Map<String, Boolean> testConnections = new HashMap<>();
        testConnections.put(testContextOne, false);
        testConnections.put(testContextTwo, false);

        when(connectionManager.connections()).thenReturn(testConnections);

        Health.Builder healthBuilder = new Health.Builder();

        testSubject.doHealthCheck(healthBuilder);

        Health result = healthBuilder.build();
        assertThat(result.getStatus().getCode()).isEqualTo(Status.DOWN.getCode());

        Map<String, Object> resultDetails = result.getDetails();
        assertThat(resultDetails).isNotEmpty();
        assertThat(resultDetails.size()).isEqualTo(2);
        String detailsContextOne = (String) resultDetails.get(expectedDetailsContextOne);
        assertThat(detailsContextOne).isNotNull();
        assertThat(detailsContextOne).isEqualTo(Status.DOWN.getCode());
        String detailsContextTwo = (String) resultDetails.get(expectedDetailsContextTwo);
        assertThat(detailsContextTwo).isNotNull();
        assertThat(detailsContextTwo).isEqualTo(Status.DOWN.getCode());
    }
}