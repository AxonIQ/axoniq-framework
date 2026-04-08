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

package org.axonframework.messaging.core.annotation;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.messaging.eventhandling.annotation.SequenceNumberParameterResolverFactory;
import org.axonframework.messaging.core.configuration.reflection.ConfigurationParameterResolverConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.reflection.ConfigurationParameterResolverFactory;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationParameterResolverConfigurationEnhancerTest {

    @Test
    void addsConfigurationParameterResolverFactory() {
        DefaultComponentRegistry componentRegistry = new DefaultComponentRegistry();
        componentRegistry.disableEnhancerScanning()
                         .registerEnhancer(new ConfigurationParameterResolverConfigurationEnhancer())
                         .registerComponent(ParameterResolverFactory.class,
                                            (c) -> new SequenceNumberParameterResolverFactory());


        Configuration build = componentRegistry.build(Mockito.mock(LifecycleRegistry.class));

        ParameterResolverFactory factory = build.getComponent(ParameterResolverFactory.class);
        assertInstanceOf(MultiParameterResolverFactory.class, factory);
        assertEquals(2, ((MultiParameterResolverFactory) factory).getDelegates().size());
        assertInstanceOf(SequenceNumberParameterResolverFactory.class,
                         ((MultiParameterResolverFactory) factory).getDelegates().get(0));
        assertInstanceOf(ConfigurationParameterResolverFactory.class,
                         ((MultiParameterResolverFactory) factory).getDelegates().get(1));
    }
}