/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantmanagement;

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringTenantConfiguration {

    // tag::tenant-connect-predicate[]
    @Bean
    public TenantConnectPredicate tenantConnectPredicate() {
        return tenant -> tenant.tenantId().startsWith("tenant-");
    }
    // end::tenant-connect-predicate[]

    // tag::metadata-tenant-resolver[]
    @Bean
    public TenantResolver tenantResolver() {
        return new MetadataBasedTenantResolver("tenant");
    }
    // end::metadata-tenant-resolver[]
}
