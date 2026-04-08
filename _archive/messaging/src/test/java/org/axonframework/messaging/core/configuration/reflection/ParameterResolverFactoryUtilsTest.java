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

package org.axonframework.messaging.core.configuration.reflection;

import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.annotation.MultiParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.junit.jupiter.api.*;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.*;

class ParameterResolverFactoryUtilsTest {

    @Test
    void registersParameterResolverAsComponentIfNonKnown() {
        DefaultComponentRegistry componentRegistry = new DefaultComponentRegistry();
        ParameterResolverFactory factory = Mockito.mock(ParameterResolverFactory.class);
        componentRegistry.disableEnhancerScanning();
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                (c) -> factory
        );

        Configuration build = componentRegistry.build(Mockito.mock(LifecycleRegistry.class));

        ParameterResolverFactory actualFactory = build.getComponent(ParameterResolverFactory.class);
        assertSame(factory, actualFactory);
    }

    @Test
    void registersParameterResolverAsDecoratorWithMultiParameterResolverFactoryIfAlreadyHas() {
        DefaultComponentRegistry componentRegistry = new DefaultComponentRegistry();
        componentRegistry.disableEnhancerScanning();
        ParameterResolverFactory factory1 = Mockito.mock(ParameterResolverFactory.class);
        ParameterResolverFactory factory2 = Mockito.mock(ParameterResolverFactory.class);
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                (c) -> factory1

        );
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                (c) -> factory2

        );

        Configuration build = componentRegistry.build(Mockito.mock(LifecycleRegistry.class));

        ParameterResolverFactory actualFactory = build.getComponent(ParameterResolverFactory.class);
        assertInstanceOf(MultiParameterResolverFactory.class, actualFactory);
        assertTrue(((MultiParameterResolverFactory) actualFactory).getDelegates().contains(factory1));
        assertTrue(((MultiParameterResolverFactory) actualFactory).getDelegates().contains(factory2));
    }
}