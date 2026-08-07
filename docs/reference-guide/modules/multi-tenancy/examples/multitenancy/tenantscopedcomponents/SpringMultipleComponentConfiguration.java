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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class SpringMultipleComponentConfiguration {

    // tag::register-multiple[]
    @Bean
    public TenantComponentProvider<CourseStatisticsStore> courseStatisticsStore() {  // <1>
        return TenantComponentProvider.withFactory(
                CourseStatisticsStore.class,
                tenant -> new InMemoryCourseStatisticsStore(tenant.tenantId()));
    }

    @Bean
    public TenantComponentProvider<DataSource> reportingDataSource() {              // <2>
        return TenantComponentProvider.withFactory(
                DataSource.class,
                tenant -> buildDataSource(tenant));
    }
    // end::register-multiple[]

    private DataSource buildDataSource(TenantDescriptor tenant) {
        return null; // Elided: application-specific lookup of the tenant's datasource.
    }
}
