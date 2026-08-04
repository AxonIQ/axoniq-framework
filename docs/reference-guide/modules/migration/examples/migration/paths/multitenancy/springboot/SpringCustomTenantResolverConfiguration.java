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

package migration.paths.multitenancy.springboot;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.messaging.core.Message;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Resolving the tenant from something other than message metadata.
 */
@Configuration
public class SpringCustomTenantResolverConfiguration {

    // tag::custom-tenant-resolver[]
    @Bean
    public TenantResolver tenantResolver() {
        return (message, tenants) -> {
            String tenantId = extractTenantId(message);                          // <1>
            if (tenantId == null) {
                throw new TenantNotResolvedException("Could not resolve a tenant for the message");
            }
            return TenantDescriptor.tenantWithId(tenantId);
        };
    }
    // end::custom-tenant-resolver[]

    private String extractTenantId(Message message) {
        return null; // Elided: application-specific lookup, from the payload or a transport header.
    }
}
