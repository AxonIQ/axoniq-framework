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

/**
 * Functional interface for looking up a component instance for a given tenant.
 *
 * @param <T> the type of the component instance
 * @author Jan Galinski
 * @since 5.3.1
 */
@FunctionalInterface
public interface TenantComponentLookup<T> {

    /**
     * Returns the component instance for the given {@code tenant}.
     *
     * @param tenant the tenant to provide the component instance for
     * @return the component instance belonging to the given {@code tenant}
     * @throws TenantNotResolvedException if the given {@code tenant} cannot be resolved
     */
    T componentFor(TenantDescriptor tenant);
}
