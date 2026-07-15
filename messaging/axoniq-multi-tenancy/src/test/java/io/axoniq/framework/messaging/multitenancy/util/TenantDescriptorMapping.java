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

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class TenantDescriptorMapping<T> implements Function<TenantDescriptor, T>, TenantDescriptors {

    private final Map<TenantDescriptor, T> delegate = new ConcurrentHashMap<>();

    public <S extends T> S entry(TenantDescriptor key, S value) {
        delegate.put(key, value);
        return value;
    }

    @Override
    public T apply(TenantDescriptor tenantDescriptor) {
        return delegate.get(tenantDescriptor);
    }

    @Override
    public List<TenantDescriptor> tenants() {
        return List.copyOf(delegate.keySet());
    }
}
