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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StaticTenantConnectPredicateTest {

    @Nested
    class ConfiguredTenantIds {

        @Test
        void acceptsOnlyTenantIdsFromTheFixedSet() {
            // given
            StaticTenantConnectPredicate testSubject = new StaticTenantConnectPredicate(Set.of("tenant-a", "tenant-b"));

            // then
            assertThat(testSubject).accepts(TenantDescriptor.tenantWithId("tenant-a"),
                                            TenantDescriptor.tenantWithId("tenant-b"))
                                   .rejects(TenantDescriptor.tenantWithId("tenant-c"));
        }

        @Test
        void parsesTrimmedTenantIdsFromCsv() {
            // given
            StaticTenantConnectPredicate testSubject = StaticTenantConnectPredicate.from(" tenant-a,tenant-b, ,");

            // then
            assertThat(testSubject).accepts(TenantDescriptor.tenantWithId("tenant-a"),
                                            TenantDescriptor.tenantWithId("tenant-b"))
                                   .rejects(TenantDescriptor.tenantWithId("tenant-c"));
        }

        @ParameterizedTest
        @NullAndEmptySource
        void doesNotAcceptEmptyOrNull(@Nullable Set<String> tenantIds) {
            assertThatThrownBy(() -> new StaticTenantConnectPredicate(tenantIds))
                    .hasMessage("Tenant identifiers are required");
        }

        @Test
        void doesNotAcceptNullCsv() {
            assertThatThrownBy(() -> StaticTenantConnectPredicate.from(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("Tenant identifiers are required");
        }
    }
}
