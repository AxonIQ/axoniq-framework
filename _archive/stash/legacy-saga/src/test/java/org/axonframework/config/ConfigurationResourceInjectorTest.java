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

package org.axonframework.config;

import jakarta.inject.Inject;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ConfigurationResourceInjector}.
 */
class ConfigurationResourceInjectorTest {

    private Configuration configuration;
    private ConfigurationResourceInjector testSubject;

    @BeforeEach
    void setUp() {
        configuration = MessagingConfigurer.create().build();
        testSubject = new ConfigurationResourceInjector(configuration);
    }

    @Test
    void injectorHasResource() {
        Saga saga = new Saga();
        testSubject.injectResources(saga);

        assertSame(configuration.getComponent(CommandBus.class), saga.commandBus);
        assertSame(configuration.getComponent(CommandGateway.class), saga.commandGateway);
        assertSame(configuration.getComponent(QueryGateway.class), saga.queryGateway);
        assertNull(saga.inexistent);
    }

    public static class Saga {

        @Inject
        private CommandBus commandBus;

        @Inject
        private String inexistent;

        private CommandGateway commandGateway;
        private QueryGateway queryGateway;

        @Inject
        public void setCommandGateway(CommandGateway commandGateway) {
            this.commandGateway = commandGateway;
        }

        @Inject
        public void setEventStore(QueryGateway queryGateway) {
            this.queryGateway = queryGateway;
        }
    }
}
