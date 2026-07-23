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

import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.junit.jupiter.api.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantScopedCacheTest {

    // Each create returns a distinct value, and per-tenant creation counts are recorded, so caching and eviction
    // are observable through both instance identity and how often the factory ran.
    private final ConcurrentHashMap<TenantDescriptor, AtomicInteger> creations = new ConcurrentHashMap<>();
    private final Function<TenantDescriptor, String> countingFactory = tenant -> {
        int creation = creations.computeIfAbsent(tenant, ignored -> new AtomicInteger()).incrementAndGet();
        return tenant.tenantId() + "#" + creation;
    };
    private final TenantScopedCache<String> testSubject = new TenantScopedCache<>(countingFactory);

    @Nested
    class Creation {

        @Test
        void appliesTheFactoryOncePerTenantAndCachesTheResult() {
            String first = testSubject.componentFor(TENANT_A);
            String second = testSubject.componentFor(TENANT_A);

            assertThat(second).isSameAs(first);
            assertThat(creations.get(TENANT_A)).hasValue(1);
        }

        @Test
        void buildsADistinctComponentPerTenant() {
            assertThat(testSubject.componentFor(TENANT_A)).isNotEqualTo(testSubject.componentFor(TENANT_B));
        }

        @Test
        void rejectsANullFactory() {
            assertThatThrownBy(() -> new TenantScopedCache<>(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Eviction {

        @Test
        void cancellingATenantRegistrationEvictsItsCachedComponent() {
            String before = testSubject.componentFor(TENANT_A);
            Registration registration = testSubject.registerTenant(TENANT_A);

            assertThat(registration.cancel()).isTrue();

            String rebuilt = testSubject.componentFor(TENANT_A);
            assertThat(rebuilt).isNotEqualTo(before);
            assertThat(creations.get(TENANT_A)).hasValue(2);
        }

        @Test
        void registerAndStartTenantEvictsOnCancelAsWell() {
            testSubject.componentFor(TENANT_A);
            Registration registration = testSubject.registerAndStartTenant(TENANT_A);

            assertThat(registration.cancel()).isTrue();
            assertThat(testSubject.componentFor(TENANT_A)).isEqualTo(TENANT_A.tenantId() + "#2");
        }

        @Test
        void cancellingWithoutACachedComponentReportsNothingEvicted() {
            Registration registration = testSubject.registerTenant(TENANT_A);

            assertThat(registration.cancel()).isFalse();
        }
    }

    @Test
    void describesItsCachedTenants() {
        testSubject.componentFor(TENANT_A);
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        assertThat(descriptor.getDescribedProperties()).containsKey("tenants");
    }
}
