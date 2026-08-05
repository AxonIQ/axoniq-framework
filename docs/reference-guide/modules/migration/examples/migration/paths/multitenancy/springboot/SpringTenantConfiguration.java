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

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import migration.paths.multitenancy.CourseStatisticsStore;
import migration.paths.multitenancy.JdbcCourseStatisticsStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * The Axon Framework 5 replacements for the Axon Framework 4 {@code extension-multitenancy} configuration, expressed
 * as Spring beans.
 */
@Configuration
public class SpringTenantConfiguration {

    // tag::predicate-by-convention[]
    @Bean
    public TenantConnectPredicate tenantConnectPredicate() {
        return tenant -> tenant.tenantId().startsWith("tenant-");
    }
    // end::predicate-by-convention[]

    // tag::metadata-tenant-resolver[]
    @Bean
    public TenantResolver tenantResolver() {
        return new MetadataBasedTenantResolver("tenant");
    }
    // end::metadata-tenant-resolver[]

    // tag::register-tenant-scoped-store[]
    @Bean
    public TenantComponentProvider<CourseStatisticsStore> courseStatisticsStoreProvider() { // <1>
        return TenantComponentProvider.withFactory(
                CourseStatisticsStore.class,                                               // <2>
                tenant -> new JdbcCourseStatisticsStore(dataSourceFor(tenant)));
    }
    // end::register-tenant-scoped-store[]

    private DataSource dataSourceFor(TenantDescriptor tenant) {
        return null; // Elided: application-specific lookup of the tenant's datasource.
    }
}
