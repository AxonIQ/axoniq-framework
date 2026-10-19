/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import java.util.List;

/**
 * Interface for components that can provide a list of tenant descriptors.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
@FunctionalInterface
public interface TenantDescriptors {

    /**
     * Returns a list of tenant descriptors.
     *
     * @return the list of tenant descriptors
     */
    List<TenantDescriptor> tenants();

    /**
     * Indicates whether the given {@code tenant} is one of the {@link #tenants() tenants} of this application.
     * <p>
     * Answered on every message that carries a tenant, so an implementation backed by a set should override this to
     * test membership directly rather than through the list {@link #tenants()} builds.
     *
     * @param tenant the tenant to check
     * @return {@code true} when the given {@code tenant} is known
     * @since 5.3.0
     */
    default boolean isKnown(TenantDescriptor tenant) {
        return tenants().contains(tenant);
    }
}
