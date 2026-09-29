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

import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.Assert;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link TenantConnectPredicate} that accepts a fixed set of tenant identifiers.
 * <p>
 * Applications using the Configuration API load their configuration properties themselves and pass the property's
 * comma-separated value to {@link #from(String)} when registering an instance as the
 * {@link TenantConnectPredicate} component.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
public class StaticTenantConnectPredicate implements TenantConnectPredicate {

    /**
     * The property containing a comma-separated list of tenant identifiers.
     */
    public static final String TENANTS_PROPERTY = "axoniq.multitenancy.tenants";
    private static final String ERROR_MSG = "Tenant identifiers are required";

    private final Set<String> tenants;

    /**
     * Creates a predicate from a comma-separated list of tenant identifiers.
     * <p>
     * Whitespace around an identifier and empty entries are ignored.
     *
     * @param tenantsCsv the comma-separated tenant identifiers
     * @return a predicate accepting the tenant identifiers in {@code tenantsCsv}
     */
    public static StaticTenantConnectPredicate from(String tenantsCsv) {
        Objects.requireNonNull(tenantsCsv, ERROR_MSG);
        return new StaticTenantConnectPredicate(Arrays.stream(tenantsCsv.split(","))
                                                    .map(String::trim)
                                                    .filter(tenant -> !tenant.isEmpty())
                                                    .collect(Collectors.toUnmodifiableSet()));
    }

    /**
     * Creates a predicate accepting the given tenant identifiers.
     *
     * @param tenants the tenant identifiers to accept
     */
    public StaticTenantConnectPredicate(Set<String> tenants) {
        Assert.isTrue(!Objects.requireNonNull(tenants, ERROR_MSG).isEmpty(), () -> ERROR_MSG);
        this.tenants = Set.copyOf(tenants);
    }

    /**
     * Tests whether the tenant identifier is in this predicate's fixed set.
     *
     * @param tenantDescriptor the tenant to test
     * @return {@code true} when the tenant identifier is configured
     */
    @Override
    public boolean test(TenantDescriptor tenantDescriptor) {
        return tenants.contains(tenantDescriptor.tenantId());
    }
}
