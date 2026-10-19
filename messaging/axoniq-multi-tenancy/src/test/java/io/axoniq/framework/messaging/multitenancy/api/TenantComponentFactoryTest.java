/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TenantComponentFactoryTest {

    @Nested
    class DefaultDestroy {

        @Test
        void closesAutoCloseableComponents() {
            // given
            AtomicBoolean closed = new AtomicBoolean();
            TenantComponentFactory<AutoCloseable> factory = tenant -> () -> closed.set(true);
            AutoCloseable component = factory.create(TENANT_A);

            // when
            factory.destroy(TENANT_A, component);

            // then
            assertThat(closed).isTrue();
        }

        @Test
        void ignoresComponentsThatAreNotAutoCloseable() {
            // given
            TenantComponentFactory<String> factory = TenantDescriptor::tenantId;

            // when / then
            assertThatCode(() -> factory.destroy(TENANT_A, "plain-component")).doesNotThrowAnyException();
        }

        @Test
        void swallowsExceptionsThrownWhileClosing() {
            // given
            TenantComponentFactory<AutoCloseable> factory = tenant -> () -> {
                throw new IllegalStateException("close failed");
            };
            AutoCloseable component = factory.create(TENANT_A);

            // when / then the failure is logged, not propagated, so remaining tenants still get cleaned up
            assertThatCode(() -> factory.destroy(TENANT_A, component)).doesNotThrowAnyException();
        }
    }
}
