/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantscopedcomponents;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringComponentConfiguration {

    // tag::register-provider[]
    @Bean
    public TenantComponentProvider<CourseStatisticsStore> courseStatisticsStoreProvider() { // <1>
        return TenantComponentProvider.withFactory(
                CourseStatisticsStore.class,                                                 // <2>
                tenant -> new InMemoryCourseStatisticsStore(tenant.tenantId())
        );
    }
    // end::register-provider[]
}
