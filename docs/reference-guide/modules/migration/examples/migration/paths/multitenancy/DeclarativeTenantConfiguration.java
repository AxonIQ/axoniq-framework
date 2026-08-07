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

package migration.paths.multitenancy;

import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

/**
 * The Axon Framework 5 replacements for the Axon Framework 4 {@code extension-multitenancy} configuration, expressed
 * through the Configuration API.
 */
public class DeclarativeTenantConfiguration {

    // tag::disable-multi-tenancy[]
    public void disableMultiTenancy(MessagingConfigurer configurer) {
        configurer.componentRegistry(MultiTenancyUtils::disable);
    }
    // end::disable-multi-tenancy[]

    // tag::predicate-by-convention[]
    public void registerTenantFilter(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantConnectPredicate.class,
                config -> tenant -> tenant.tenantId().startsWith("tenant-")));
    }
    // end::predicate-by-convention[]

    // tag::predicate-from-fixed-set[]
    public void registerFixedTenantSet(MessagingConfigurer configurer) {
        Set<String> tenantContexts = Set.of("tenant-a", "tenant-b", "tenant-c");
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantConnectPredicate.class,
                config -> tenant -> tenantContexts.contains(tenant.tenantId())));
    }
    // end::predicate-from-fixed-set[]

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

    // tag::register-tenant-scoped-store[]
    public void registerTenantScopedRepository(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantComponentProvider.class,                                   // <1>
                config -> TenantComponentProvider.withFactory(
                        CourseStatisticsStore.class,                             // <2>
                        tenant -> new JdbcCourseStatisticsStore(dataSourceFor(tenant)))));
    }
    // end::register-tenant-scoped-store[]

    private String extractTenantId(Message message) {
        return null; // Elided: application-specific lookup, from the payload or a transport header.
    }

    private DataSource dataSourceFor(TenantDescriptor tenant) {
        return null; // Elided: application-specific lookup of the tenant's datasource.
    }
}
