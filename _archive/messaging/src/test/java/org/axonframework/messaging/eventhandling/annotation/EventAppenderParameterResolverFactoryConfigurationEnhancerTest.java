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

package org.axonframework.messaging.eventhandling.annotation;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.StubLifecycleRegistry;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link EventAppenderParameterResolverFactoryConfigurationEnhancer}.
 *
 * @author Mitchell Herrijgers.
 */
class EventAppenderParameterResolverFactoryConfigurationEnhancerTest {

    @Test
    void registersParameterResolverToComponentRegistry() {
        DefaultComponentRegistry registry = new DefaultComponentRegistry();
        registry.disableEnhancerScanning();
        registry.registerEnhancer(new EventAppenderParameterResolverFactoryConfigurationEnhancer());

        Configuration configuration = registry.build(new StubLifecycleRegistry());

        ParameterResolverFactory factory = configuration.getComponent(ParameterResolverFactory.class);
        assertInstanceOf(EventAppenderParameterResolverFactory.class, factory);
    }
}