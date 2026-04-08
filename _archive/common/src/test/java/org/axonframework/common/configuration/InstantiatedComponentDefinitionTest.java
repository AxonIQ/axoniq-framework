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

import org.junit.jupiter.api.*;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test validating the {@link InstantiatedComponentDefinition}.
 *
 * @author Allard Buijze
 */
class InstantiatedComponentDefinitionTest extends ComponentTestSuite<InstantiatedComponentDefinition<String>> {

    @Override
    InstantiatedComponentDefinition<String> createComponent(Component.Identifier<String> id,
                                                            String instance) {
        return new InstantiatedComponentDefinition<>(id, instance);
    }

    @Override
    InstantiatedComponentDefinition<String> createComponent(Component.Identifier<String> id,
                                                            ComponentBuilder<String> builder) {
        Assumptions.abort("InstantiatedComponentDefinition doesn't support creation with builder.");
        return null;
    }

    @Override
    void registerStartHandler(InstantiatedComponentDefinition<String> testSubject,
                              int phase,
                              BiConsumer<Configuration, String> handler) {
        testSubject.onStart(phase, handler);
    }

    @Override
    void registerShutdownHandler(InstantiatedComponentDefinition<String> testSubject,
                                 int phase,
                                 BiConsumer<Configuration, String> handler) {
        testSubject.onShutdown(phase, handler);
    }

    @Test
    void constructorThrowsNullPointerExceptionForNullIdentifier() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new InstantiatedComponentDefinition<>(null, "testValue"));
    }

    @Test
    void constructorThrowsNullPointerExceptionForNullInstance() {
        Component.Identifier<String> testId = new Component.Identifier<>(String.class, "value");
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new InstantiatedComponentDefinition<>(testId, null));
    }
}
