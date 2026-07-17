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

package io.axoniq.framework.axonserver.connector.api;

import org.junit.jupiter.api.*;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.builder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test validating the {@link AxonServerConfiguration}.
 */
class AxonServerConfigurationTest {

    @Test
    void eventsFlowControl() {
        AxonServerConfiguration axonServerConfiguration = builder().eventFlowControl(10, 20, 30).build();

        assertThat(axonServerConfiguration.getEventFlowControl().getPermits()).isEqualTo(10);
        assertThat(axonServerConfiguration.getEventFlowControl().getNrOfNewPermits()).isEqualTo(20);
        assertThat(axonServerConfiguration.getEventFlowControl().getNewPermitsThreshold()).isEqualTo(30);
        assertThat(axonServerConfiguration.getPermits()).isEqualTo(5000);
        assertThat(axonServerConfiguration.getNrOfNewPermits()).isEqualTo(2500);
        assertThat(axonServerConfiguration.getNewPermitsThreshold()).isEqualTo(2500);
    }

    @Test
    void commandFlowControl() {
        AxonServerConfiguration axonServerConfiguration = builder().commandFlowControl(10, 20, 30).build();

        assertThat(axonServerConfiguration.getCommandFlowControl().getPermits()).isEqualTo(10);
        assertThat(axonServerConfiguration.getCommandFlowControl().getNrOfNewPermits()).isEqualTo(20);
        assertThat(axonServerConfiguration.getCommandFlowControl().getNewPermitsThreshold()).isEqualTo(30);
        assertThat(axonServerConfiguration.getPermits()).isEqualTo(5000);
        assertThat(axonServerConfiguration.getNrOfNewPermits()).isEqualTo(2500);
        assertThat(axonServerConfiguration.getNewPermitsThreshold()).isEqualTo(2500);
    }

    @Test
    void queryFlowControl() {
        AxonServerConfiguration axonServerConfiguration = builder().queryFlowControl(10, 20, 30).build();

        assertThat(axonServerConfiguration.getQueryFlowControl().getPermits()).isEqualTo(10);
        assertThat(axonServerConfiguration.getQueryFlowControl().getNrOfNewPermits()).isEqualTo(20);
        assertThat(axonServerConfiguration.getQueryFlowControl().getNewPermitsThreshold()).isEqualTo(30);
        assertThat(axonServerConfiguration.getPermits()).isEqualTo(5000);
        assertThat(axonServerConfiguration.getNrOfNewPermits()).isEqualTo(2500);
        assertThat(axonServerConfiguration.getNewPermitsThreshold()).isEqualTo(2500);
    }
}