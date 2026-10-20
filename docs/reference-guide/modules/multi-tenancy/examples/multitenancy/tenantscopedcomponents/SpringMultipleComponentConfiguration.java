/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
