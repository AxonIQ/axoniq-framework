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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test validating the {@link SimpleModule}.
 *
 * @author Steven van Beelen
 */
class BaseModuleTest {

    private SimpleModule testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new SimpleModule("simple-module");
    }

    @Test
    void simpleModuleDelegatesToComponentRegistry() {
        AtomicReference<ComponentRegistry> detected = new AtomicReference<>();
        testSubject.componentRegistry(detected::set);
        var registry = new StubLifecycleRegistry();
        var config = new DefaultComponentRegistry().build(registry);
        testSubject.build(config, registry);
        assertNotNull(detected.get());
    }

    @Test
    void throwsIllegalArgumentExceptionForEmptyNameString() {
        assertThrows(IllegalArgumentException.class, () -> new SimpleModule(""));
    }

    @Test
    void throwsIllegalArgumentExceptionForNullNameString() {
        assertThrows(IllegalArgumentException.class, () -> new SimpleModule(null));
    }

    @Test
    void simpleModuleDoesNotAllowToRegisterComponentRegistryHooksAfterBuild() {
        AtomicReference<ComponentRegistry> detected = new AtomicReference<>();
        var registry = new StubLifecycleRegistry();
        var config = new DefaultComponentRegistry().build(registry);
        testSubject.build(config, registry);

        var exc = assertThrows(IllegalStateException.class, () -> testSubject.componentRegistry(detected::set));
        assertThat(exc.getMessage()).isEqualTo("Module has already been built.");
        assertNull(detected.get());
    }

    private static class SimpleModule extends BaseModule<SimpleModule> {

        public SimpleModule(String name) {
            super(name);
        }
    }
}