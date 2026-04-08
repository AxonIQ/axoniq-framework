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

import org.junit.jupiter.api.*;
import org.mockito.*;

import java.lang.reflect.Parameter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HierarchicalParameterResolverFactoryTest {

    @Test
    void resolvesComponentFromChildIfExistsInBoth() throws NoSuchMethodException {
        ParameterResolverFactory parent = Mockito.mock(ParameterResolverFactory.class);
        //noinspection rawtypes
        FixedValueParameterResolver resolverParent = new FixedValueParameterResolver<>("parent");
        //noinspection unchecked
        when(parent.createInstance(any(), any(), eq(0))).thenReturn(resolverParent);

        ParameterResolverFactory child = Mockito.mock(ParameterResolverFactory.class);
        //noinspection rawtypes
        FixedValueParameterResolver resolverChild = new FixedValueParameterResolver<>("child");
        //noinspection unchecked
        when(child.createInstance(any(), any(), eq(0))).thenReturn(resolverChild);

        HierarchicalParameterResolverFactory factory = HierarchicalParameterResolverFactory.create(parent, child);

        ParameterResolver<?> result = factory.createInstance(this.getClass().getDeclaredMethod(
                "resolvesComponentFromChildIfExistsInBoth"), new Parameter[]{}, 0);
        assertSame(resolverChild, result);
    }


    @Test
    void resolvesComponentFromParentIfDoesntExistInChild() throws NoSuchMethodException {
        ParameterResolverFactory parent = Mockito.mock(ParameterResolverFactory.class);
        //noinspection rawtypes
        FixedValueParameterResolver resolverParent = new FixedValueParameterResolver<>("parent");
        //noinspection unchecked
        when(parent.createInstance(any(), any(), eq(0))).thenReturn(resolverParent);

        ParameterResolverFactory child = Mockito.mock(ParameterResolverFactory.class);
        when(child.createInstance(any(), any(), eq(0))).thenReturn(null);

        HierarchicalParameterResolverFactory factory = HierarchicalParameterResolverFactory.create(parent, child);

        ParameterResolver<?> result = factory.createInstance(this.getClass().getDeclaredMethod(
                "resolvesComponentFromParentIfDoesntExistInChild"), new Parameter[]{}, 0);
        assertSame(resolverParent, result);
    }
}