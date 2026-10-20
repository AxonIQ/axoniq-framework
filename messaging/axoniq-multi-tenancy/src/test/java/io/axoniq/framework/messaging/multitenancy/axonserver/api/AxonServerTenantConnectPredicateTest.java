/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.api;

import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

class AxonServerTenantConnectPredicateTest {

    @Nested
    class TestPredicate {

        private final TenantConnectPredicate testSubject = new AxonServerTenantConnectPredicate();

        @Test
        void acceptsTenantWithoutProperties() {
            // given
            TenantDescriptor tenant = TenantDescriptor.tenantWithId("tenant-a");

            // then
            assertThat(testSubject).accepts(tenant);
        }

        @Test
        void rejectsAdminContext() {
            // given
            TenantDescriptor tenant = TenantDescriptor.tenantWithId(ADMIN_CONTEXT);

            // then
            assertThat(testSubject).rejects(tenant);
        }

        @Test
        void acceptsTenantWithProperties() {
            // given
            TenantDescriptor tenant = new TenantDescriptor(
                    "tenant-b",
                    Map.of("replicationGroup", "default")
            );

            // then
            assertThat(testSubject).accepts(tenant);
        }

        @Test
        void keepsEveryNonAdminTenantWhenUsedAsFilter() {
            // given
            TenantDescriptor tenantWithoutProperties = TenantDescriptor.tenantWithId("tenant-a");
            TenantDescriptor adminContext = TenantDescriptor.tenantWithId(ADMIN_CONTEXT);
            TenantDescriptor tenantWithProperties = new TenantDescriptor(
                    "tenant-b",
                    Map.of("replicationGroup", "default")
            );

            // when
            List<TenantDescriptor> result = Stream.of(tenantWithoutProperties, adminContext, tenantWithProperties)
                                                  .filter(testSubject)
                                                  .toList();

            // then
            assertThat(result).containsExactly(tenantWithoutProperties, tenantWithProperties);
        }
    }
}
