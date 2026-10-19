/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
