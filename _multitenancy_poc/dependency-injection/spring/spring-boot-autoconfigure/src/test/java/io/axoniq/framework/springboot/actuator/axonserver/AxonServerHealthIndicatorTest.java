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

package io.axoniq.framework.springboot.actuator.axonserver;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.actuator.HealthStatus;
import org.junit.jupiter.api.*;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
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
        assertEquals(Status.UP.getCode(), result.getStatus().getCode());

        Map<String, Object> resultDetails = result.getDetails();
        assertFalse(resultDetails.isEmpty());
        assertEquals(2, resultDetails.size());
        String detailsContextOne = (String) resultDetails.get(expectedDetailsContextOne);
        assertNotNull(detailsContextOne);
        assertEquals(Status.UP.getCode(), detailsContextOne);
        String detailsContextTwo = (String) resultDetails.get(expectedDetailsContextTwo);
        assertNotNull(detailsContextTwo);
        assertEquals(Status.UP.getCode(), detailsContextTwo);
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
        assertEquals(HealthStatus.WARN, result.getStatus());

        Map<String, Object> resultDetails = result.getDetails();
        assertFalse(resultDetails.isEmpty());
        assertEquals(2, resultDetails.size());
        String detailsContextOne = (String) resultDetails.get(expectedDetailsContextOne);
        assertNotNull(detailsContextOne);
        assertEquals(Status.UP.getCode(), detailsContextOne);
        String detailsContextTwo = (String) resultDetails.get(expectedDetailsContextTwo);
        assertNotNull(detailsContextTwo);
        assertEquals(Status.DOWN.getCode(), detailsContextTwo);
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
        assertEquals(Status.DOWN.getCode(), result.getStatus().getCode());

        Map<String, Object> resultDetails = result.getDetails();
        assertFalse(resultDetails.isEmpty());
        assertEquals(2, resultDetails.size());
        String detailsContextOne = (String) resultDetails.get(expectedDetailsContextOne);
        assertNotNull(detailsContextOne);
        assertEquals(Status.DOWN.getCode(), detailsContextOne);
        String detailsContextTwo = (String) resultDetails.get(expectedDetailsContextTwo);
        assertNotNull(detailsContextTwo);
        assertEquals(Status.DOWN.getCode(), detailsContextTwo);
    }
}