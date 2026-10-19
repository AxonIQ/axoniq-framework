/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.messaging.multitenancy.api;

import java.util.function.Predicate;

/**
 * Predicate that during runtime determines whether a newly registered {@link TenantDescriptor tenant} should be added
 * to the tenant-aware infrastructure components. Used for dynamic registration of tenant-specific components.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */
@FunctionalInterface
public interface TenantConnectPredicate extends Predicate<TenantDescriptor> {
}
