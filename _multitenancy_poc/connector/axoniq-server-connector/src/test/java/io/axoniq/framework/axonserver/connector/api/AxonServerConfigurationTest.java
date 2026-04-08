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

package io.axoniq.framework.axonserver.connector.api;

import org.junit.jupiter.api.*;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.builder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test validating the {@link AxonServerConfiguration}.
 */
class AxonServerConfigurationTest {

    @Test
    void eventsFlowControl() {
        AxonServerConfiguration axonServerConfiguration = builder().eventFlowControl(10, 20, 30).build();

        assertEquals(10, axonServerConfiguration.getEventFlowControl().getPermits());
        assertEquals(20, axonServerConfiguration.getEventFlowControl().getNrOfNewPermits());
        assertEquals(30, axonServerConfiguration.getEventFlowControl().getNewPermitsThreshold());
        assertEquals(5000, axonServerConfiguration.getPermits());
        assertEquals(2500, axonServerConfiguration.getNrOfNewPermits());
        assertEquals(2500, axonServerConfiguration.getNewPermitsThreshold());
    }

    @Test
    void commandFlowControl() {
        AxonServerConfiguration axonServerConfiguration = builder().commandFlowControl(10, 20, 30).build();

        assertEquals(10, axonServerConfiguration.getCommandFlowControl().getPermits());
        assertEquals(20, axonServerConfiguration.getCommandFlowControl().getNrOfNewPermits());
        assertEquals(30, axonServerConfiguration.getCommandFlowControl().getNewPermitsThreshold());
        assertEquals(5000, axonServerConfiguration.getPermits());
        assertEquals(2500, axonServerConfiguration.getNrOfNewPermits());
        assertEquals(2500, axonServerConfiguration.getNewPermitsThreshold());
    }

    @Test
    void queryFlowControl() {
        AxonServerConfiguration axonServerConfiguration = builder().queryFlowControl(10, 20, 30).build();

        assertEquals(10, axonServerConfiguration.getQueryFlowControl().getPermits());
        assertEquals(20, axonServerConfiguration.getQueryFlowControl().getNrOfNewPermits());
        assertEquals(30, axonServerConfiguration.getQueryFlowControl().getNewPermitsThreshold());
        assertEquals(5000, axonServerConfiguration.getPermits());
        assertEquals(2500, axonServerConfiguration.getNrOfNewPermits());
        assertEquals(2500, axonServerConfiguration.getNewPermitsThreshold());
    }
}