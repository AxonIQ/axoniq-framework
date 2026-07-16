/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
