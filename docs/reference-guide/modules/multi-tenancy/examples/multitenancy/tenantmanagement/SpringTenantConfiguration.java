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
