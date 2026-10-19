/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import java.util.function.Function;

/**
 * Functional interface for looking up a component instance for a given tenant.
 *
 * @param <T> the type of the component instance
 * @author Jan Galinski
 * @since 5.3.1
 */
@FunctionalInterface
public interface TenantComponentLookup<T> extends Function<TenantDescriptor, T> {

    /**
     * Returns the component instance for the given {@code tenant}.
     *
     * @param tenant the tenant to provide the component instance for
     * @return the component instance belonging to the given {@code tenant}
     * @throws TenantNotResolvedException if the given {@code tenant} cannot be resolved
     */
    T componentFor(TenantDescriptor tenant);

    @Override
    default T apply(TenantDescriptor tenantDescriptor) {
        return componentFor(tenantDescriptor);
    }
}
