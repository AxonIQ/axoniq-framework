/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantscopedcomponents;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;

import javax.sql.DataSource;

public class ComponentLifecycle {

    // tag::factory-destroy[]
    TenantComponentFactory<CourseStatisticsStore> factory = new TenantComponentFactory<>() {

        @Override
        public CourseStatisticsStore create(TenantDescriptor tenant) {
            return new JdbcCourseStatisticsStore(dataSourceFor(tenant));
        }

        @Override
        public void destroy(TenantDescriptor tenant, CourseStatisticsStore store) {
            store.flush();
            store.close();
        }
    };
    // end::factory-destroy[]

    private DataSource dataSourceFor(TenantDescriptor tenant) {
        return null; // Elided: application-specific lookup of the tenant's datasource.
    }
}
