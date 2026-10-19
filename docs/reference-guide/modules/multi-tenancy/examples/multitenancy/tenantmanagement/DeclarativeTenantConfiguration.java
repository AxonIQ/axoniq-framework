/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantmanagement;

import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.configuration.StaticTenantConnectPredicate;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class DeclarativeTenantConfiguration {

    // tag::disable-multi-tenancy[]
    public void disableMultiTenancy(MessagingConfigurer configurer) {
        configurer.componentRegistry(MultiTenancyUtils::disable);
    }
    // end::disable-multi-tenancy[]

    // tag::tenant-connect-predicate[]
    public void registerTenantFilter(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantConnectPredicate.class,
                config -> tenant -> tenant.tenantId().startsWith("tenant-")));
    }
    // end::tenant-connect-predicate[]

    // tag::static-tenant-connect-predicate[]
    public void registerStaticTenantFilter(MessagingConfigurer configurer, Properties properties) {
        if (!properties.containsKey("axoniq.multitenancy.tenants")) {
            return;
        }
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantConnectPredicate.class,
                config -> StaticTenantConnectPredicate.from(
                        properties.getProperty("axoniq.multitenancy.tenants"))));
    }
    // end::static-tenant-connect-predicate[]

    // tag::metadata-tenant-resolver[]
    public void registerTenantResolverForKey(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantResolver.class,
                config -> new MetadataBasedTenantResolver("tenant")));
    }
    // end::metadata-tenant-resolver[]

    // tag::custom-tenant-resolver[]
    public void registerCustomTenantResolver(MessagingConfigurer configurer) {
        TenantResolver resolver = new TenantResolver() {
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
        configurer.componentRegistry(registry -> registry.registerComponent(TenantResolver.class,
                                                                            config -> resolver));
    }
    // end::custom-tenant-resolver[]

    private String extractTenantId(Message message) {
        return message.metadata().get("x-tenant");
    }
}
