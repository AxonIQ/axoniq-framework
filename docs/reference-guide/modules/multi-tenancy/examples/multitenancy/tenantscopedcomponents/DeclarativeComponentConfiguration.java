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

package multitenancy.tenantscopedcomponents;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

import javax.sql.DataSource;

public class DeclarativeComponentConfiguration {

    // tag::register-provider[]
    public void registerTenantComponents(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry ->
            registry.registerComponent(
                    TenantComponentProvider.class,                                     // <1>
                    config -> TenantComponentProvider.withFactory(
                            CourseStatisticsStore.class,                               // <2>
                            tenant -> new InMemoryCourseStatisticsStore(tenant.tenantId())
                    )
            )
        );
    }
    // end::register-provider[]

    public void registerSeveralTenantComponents(MessagingConfigurer configurer) {
        // tag::register-multiple[]
        configurer.componentRegistry(registry -> registry
            .registerComponent(TenantComponentProvider.class, "courseStatisticsStore", // <1>
                    config -> TenantComponentProvider.withFactory(
                            CourseStatisticsStore.class,
                            tenant -> new InMemoryCourseStatisticsStore(tenant.tenantId())))
            .registerComponent(TenantComponentProvider.class, "reportingDataSource",   // <2>
                    config -> TenantComponentProvider.withFactory(
                            DataSource.class,
                            tenant -> buildDataSource(tenant)))
        );
        // end::register-multiple[]
    }

    private DataSource buildDataSource(TenantDescriptor tenant) {
        return null; // Elided: application-specific lookup of the tenant's datasource.
    }
}
