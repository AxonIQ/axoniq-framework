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

package org.axonframework.test.fixture;

import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public class AxonTestFixtureDisableAxonServerTest {

    @Test
    void disablesAxonServerForMessagingConfigurerNeverThrows() {
        var configurer = MessagingConfigurer.create();

        assertDoesNotThrow(() -> AxonTestFixture.with(configurer, AxonTestFixture.Customization::disableAxonServer));
    }

    @Test
    void disablesAxonServerForEventSourcingConfigurerNeverThrows() {
        var configurer = EventSourcingConfigurer.create();

        assertDoesNotThrow(() -> AxonTestFixture.with(configurer, AxonTestFixture.Customization::disableAxonServer));
    }
}
