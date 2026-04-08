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

import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.reflection.HierarchicalParameterResolverFactoryConfigurationEnhancer;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HierarchicalParameterResolverFactoryConfigurationEnhancerTest {

    @Test
    void createsHierarchicalParameterResolverFromParentAndChild() throws NoSuchMethodException {
        Method testMethod = this.getClass().getDeclaredMethod("createsHierarchicalParameterResolverFromParentAndChild");

        // Set up a parent with a unique ParameterResolverFactory
        DefaultComponentRegistry parent = createCleanComponentRegistry();
        ParameterResolverFactory parentParameterResolverFactory = mock(ParameterResolverFactory.class);
        parent.registerComponent(ParameterResolverFactory.class, c -> parentParameterResolverFactory);
        //noinspection rawtypes
        ParameterResolver parentParameterResolver = mock(ParameterResolver.class);
        //noinspection unchecked
        when(parentParameterResolverFactory.createInstance(eq(testMethod), eq(new Parameter[]{}), eq(0)))
                .thenReturn(parentParameterResolver);

        // Set up the child with its own ParameterResolverFactory
        DefaultComponentRegistry child = createCleanComponentRegistry();
        ParameterResolverFactory childParameterResolverFactory = mock(ParameterResolverFactory.class);
        //noinspection rawtypes
        ParameterResolver childParameterResolver = mock(ParameterResolver.class);
        child.registerComponent(ParameterResolverFactory.class, c -> childParameterResolverFactory);

        // Build both in a nested way - this will create a HierarchicalParameterResolverFactory
        Configuration parentConfiguration = parent.build(mock(LifecycleRegistry.class));
        Configuration childConfiguration = child.buildNested(parentConfiguration, mock(LifecycleRegistry.class));

        // So, we can assert the right factories are created
        ParameterResolverFactory parentFactory = parentConfiguration.getComponent(ParameterResolverFactory.class);
        ParameterResolverFactory childFactory = childConfiguration.getComponent(ParameterResolverFactory.class);
        assertNotSame(parentFactory, childFactory);
        assertSame(parentFactory, parentParameterResolverFactory);
        assertInstanceOf(HierarchicalParameterResolverFactory.class, childFactory);

        // Now, we test the hierarchy. First, with the child returning null
        when(childParameterResolverFactory.createInstance(eq(testMethod), eq(new Parameter[]{}), eq(0))).thenReturn(null);
        // The parent should just always return the parent
        assertSame(parentParameterResolver, parentFactory.createInstance(testMethod, new Parameter[]{}, 0));

        // But if the child has a result, it should return that
        //noinspection unchecked
        when(childParameterResolverFactory.createInstance(eq(testMethod), eq(new Parameter[]{}), eq(0)))
                .thenReturn(childParameterResolver);
        assertSame(childParameterResolver, childFactory.createInstance(testMethod, new Parameter[]{}, 0));
    }

    private DefaultComponentRegistry createCleanComponentRegistry() {
        DefaultComponentRegistry defaultComponentRegistry = new DefaultComponentRegistry();
        defaultComponentRegistry.disableEnhancerScanning()
                                .registerEnhancer(new HierarchicalParameterResolverFactoryConfigurationEnhancer());
        return defaultComponentRegistry;
    }
}