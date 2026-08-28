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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TenantConnectPredicateTest {

    @Nested
    class TenantIdBasedPredicate {

        @Test
        void acceptsOnlyTenantWithMatchingTenantId() {
            // given
            TenantConnectPredicate testSubject = tenant -> "tenant-b".equals(tenant.tenantId());
            TenantDescriptor matchingTenant = TenantDescriptor.tenantWithId("tenant-b");
            TenantDescriptor otherTenant = TenantDescriptor.tenantWithId("tenant-a");

            // then
            assertThat(testSubject).accepts(matchingTenant)
                                   .rejects(otherTenant);
        }

        @Test
        void keepsOnlyTenantWithMatchingTenantIdWhenUsedAsFilter() {
            // given
            TenantConnectPredicate testSubject = tenant -> "tenant-b".equals(tenant.tenantId());
            TenantDescriptor matchingTenant = new TenantDescriptor(
                    "tenant-b",
                    Map.of("replicationGroup", "default")
            );
            TenantDescriptor otherTenant = TenantDescriptor.tenantWithId("tenant-a");

            // when
            List<TenantDescriptor> result = Stream.of(otherTenant, matchingTenant)
                                                  .filter(testSubject)
                                                  .toList();

            // then
            assertThat(result).containsExactly(matchingTenant);
        }
    }
}
