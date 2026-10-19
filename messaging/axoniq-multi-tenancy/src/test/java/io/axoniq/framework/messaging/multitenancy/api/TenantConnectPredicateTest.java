/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
