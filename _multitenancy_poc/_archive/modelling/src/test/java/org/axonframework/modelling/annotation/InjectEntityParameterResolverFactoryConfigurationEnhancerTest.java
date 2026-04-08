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

package org.axonframework.modelling.annotation;

import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.*;

class InjectEntityParameterResolverFactoryConfigurationEnhancerTest {

    @Test
    void addsInjectEntityParameterResolverFactory() {

        DefaultComponentRegistry componentRegistry = new DefaultComponentRegistry();
        componentRegistry.disableEnhancerScanning()
                         .registerEnhancer(new InjectEntityParameterResolverFactoryConfigurationEnhancer());

        Configuration build = componentRegistry.build(Mockito.mock(LifecycleRegistry.class));

        ParameterResolverFactory factory = build.getComponent(ParameterResolverFactory.class);
        assertInstanceOf(InjectEntityParameterResolverFactory.class, factory);
    }
}