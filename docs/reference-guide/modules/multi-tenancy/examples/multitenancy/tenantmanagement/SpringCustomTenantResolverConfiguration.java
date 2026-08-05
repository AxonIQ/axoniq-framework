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

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.messaging.core.Message;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.Map;

@Configuration
public class SpringCustomTenantResolverConfiguration {

    // tag::custom-tenant-resolver[]
    @Bean
    public TenantResolver tenantResolver() {
        return new TenantResolver() {
            @Override
            public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
                String tenantId = message.metadata().get("x-tenant");            // <1>
                if (tenantId == null) {
                    throw new TenantNotResolvedException("Could not resolve a tenant for the message");
                }
                return TenantDescriptor.tenantWithId(tenantId);
            }

            @Override
            public Message attachTenant(Message message, TenantDescriptor tenant) {
                return message.andMetadata(Map.of("x-tenant", tenant.tenantId()));     // <2>
            }
        };
    }
    // end::custom-tenant-resolver[]

    private String extractTenantId(Message message) {
        return message.metadata().get("x-tenant");
    }
}
