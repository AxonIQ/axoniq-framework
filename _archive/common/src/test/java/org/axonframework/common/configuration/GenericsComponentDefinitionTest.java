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

package org.axonframework.common.configuration;

import org.axonframework.common.TypeReference;
import org.axonframework.common.configuration.Component;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.Configuration;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GenericsComponentDefinitionTest {

    @Test
    void canCreateComponentDefinitionWithGenericsWithBuilder() {
        MyGenericComponent<String> componentToBuild = new MyGenericComponent<>("testValue");
        ComponentDefinition<MyGenericComponent<String>> definition = ComponentDefinition.ofTypeAndName(
                new TypeReference<MyGenericComponent<String>>() {},
                "myGenericComponent"
        ).withBuilder(config -> componentToBuild);

        assertNotNull(definition);
        //noinspection rawtypes
        ComponentDefinition.ComponentCreator creator = (ComponentDefinition.ComponentCreator) definition;

        Component<?> component = creator.createComponent();
        Object resolve = component.resolve(mock(Configuration.class));
        assertSame(componentToBuild, resolve);
    }

    @Test
    void canCreateComponentDefinitionWithGenericsWithInstance() {
        MyGenericComponent<Integer> componentToBuild = new MyGenericComponent<>(42);
        ComponentDefinition<MyGenericComponent<Integer>> definition = ComponentDefinition.ofTypeAndName(
                new TypeReference<MyGenericComponent<Integer>>() {},
                "myGenericComponent"
        ).withInstance(componentToBuild);

        assertNotNull(definition);
        //noinspection rawtypes
        ComponentDefinition.ComponentCreator creator = (ComponentDefinition.ComponentCreator) definition;

        Component<?> component = creator.createComponent();
        Object resolve = component.resolve(mock(Configuration.class));
        assertSame(componentToBuild, resolve);
    }


    private static final class MyGenericComponent<T> {
        private final T value;

        public MyGenericComponent(T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }
    }
}