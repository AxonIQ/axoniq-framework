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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HierarchicalLifecycleRegistryTest {

    @Test
    void childLifecycleHandlersReceiveModuleConfigurationInsteadOfParent() {
        var parentConfiguration = mock(Configuration.class);
        var childConfiguration = mock(Configuration.class);
        var lifecycleRegistry = new StubLifecycleRegistry();
        var startHandlerCalled = new AtomicBoolean();
        var shutdownHandlerCalled = new AtomicBoolean();
        Configuration configuration = HierarchicalLifecycleRegistry.build(
                lifecycleRegistry,
                childLifecycleRegistry -> {
                    assertNotSame(lifecycleRegistry, childLifecycleRegistry);
                    childLifecycleRegistry.onStart(42, c -> {
                        assertSame(childConfiguration, c);
                        startHandlerCalled.set(true);
                    });
                    childLifecycleRegistry.onShutdown(42, c -> {
                        assertSame(childConfiguration, c);
                        shutdownHandlerCalled.set(true);
                    });
                    return childConfiguration;
                }
        );
        assertSame(childConfiguration, configuration);

        assertEquals(1, lifecycleRegistry.getStartHandlers().size());
        assertEquals(1, lifecycleRegistry.getStartHandlers().get(42).size());
        lifecycleRegistry.getStartHandlers().get(42).getFirst().run(parentConfiguration);
        assertTrue(startHandlerCalled.get());

        assertEquals(1, lifecycleRegistry.getShutdownHandlers().size());
        assertEquals(1, lifecycleRegistry.getShutdownHandlers().get(42).size());
        lifecycleRegistry.getShutdownHandlers().get(42).getFirst().run(parentConfiguration);
        assertTrue(shutdownHandlerCalled.get());
    }

    @Test
    void propagatesExceptionInLifecycleHandler() {
        var parentConfiguration = mock(Configuration.class);
        var lifecycleRegistry = new StubLifecycleRegistry();
        HierarchicalLifecycleRegistry.build(
                lifecycleRegistry,
                childLifecycleRegistry -> {
                    childLifecycleRegistry.onStart(42, (Consumer<Configuration>)c -> {
                        throw new RuntimeException("Expected exception");
                    });
                    return parentConfiguration;
                }
        );
        var result = lifecycleRegistry.getStartHandlers().get(42).getFirst().run(parentConfiguration);
        assertTrue(result.isCompletedExceptionally());
        assertInstanceOf(RuntimeException.class, result.exceptionNow());
        assertSame("Expected exception", result.exceptionNow().getMessage());

    }
}