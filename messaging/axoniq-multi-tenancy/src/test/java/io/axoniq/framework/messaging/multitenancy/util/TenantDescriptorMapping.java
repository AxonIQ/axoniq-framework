/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A test double mapping each tenant to its per-tenant component and exposing the mapped tenants as
 * {@link TenantDescriptors}, so a routing component can be given both a factory ({@code this::apply}) and the set of
 * known tenants in one object.
 */
public class TenantDescriptorMapping<T> implements Function<TenantDescriptor, T>, TenantDescriptors {

    private final Map<TenantDescriptor, T> delegate = new ConcurrentHashMap<>();

    /**
     * Maps the given {@code value} to the given {@code key}, returning it so it can be assigned in one statement.
     *
     * @param key   the tenant to map the component to
     * @param value the component of that tenant
     * @param <S>   the concrete component type, so a test can keep the recording type it passed in
     * @return the given {@code value}
     */
    public <S extends T> S entry(TenantDescriptor key, S value) {
        delegate.put(key, value);
        return value;
    }

    @Override
    public T apply(TenantDescriptor tenantDescriptor) {
        T component = delegate.get(tenantDescriptor);
        if (component == null) {
            // The factory contracts never allow null, so a routing bug names the tenant it asked for rather than
            // surfacing as a NullPointerException somewhere downstream.
            throw new IllegalStateException("No component mapped for tenant [" + tenantDescriptor.tenantId() + "]");
        }
        return component;
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(delegate.keySet());
    }
}
