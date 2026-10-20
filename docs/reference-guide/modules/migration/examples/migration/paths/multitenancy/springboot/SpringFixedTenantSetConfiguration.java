/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package migration.paths.multitenancy.springboot;

import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

/**
 * Reproducing the fixed tenant set the Axon Framework 4 {@code axon.axonserver.contexts} property used to hold.
 */
@Configuration
public class SpringFixedTenantSetConfiguration {

    // tag::predicate-from-fixed-set[]
    @Bean
    public TenantConnectPredicate tenantConnectPredicate() {
        Set<String> tenantContexts = Set.of("tenant-a", "tenant-b", "tenant-c");
        return tenant -> tenantContexts.contains(tenant.tenantId());
    }
    // end::predicate-from-fixed-set[]
}
