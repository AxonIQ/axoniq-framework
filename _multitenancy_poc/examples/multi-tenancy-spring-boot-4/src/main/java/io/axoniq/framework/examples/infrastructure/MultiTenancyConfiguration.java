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

package io.axoniq.framework.examples.infrastructure;

import io.axoniq.framework.examples.university.read.coursestats.CourseStatsRepository;
import io.axoniq.framework.examples.university.read.coursestats.InMemoryCourseStatsRepository;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantResolverRegistry;
import io.axoniq.framework.messaging.multitenancy.configuration.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolverRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Multi-tenancy wiring for the example application.
 */
@Configuration
public class MultiTenancyConfiguration {

    @Bean
    public TenantConnectPredicate tenantConnectPredicate() {
        return tenant -> tenant.tenantId().contains("-");
    }

    @Bean
    public TenantResolverRegistry tenantResolverRegistry() {
        return new DefaultTenantResolverRegistry()
                .registerResolver(c -> new MetadataBasedTenantResolver());
    }

    @Bean
    public TenantComponentRegistry<CourseStatsRepository> courseStatsRepositoryRegistry() {
        return new DefaultTenantComponentRegistry<>(
                CourseStatsRepository.class,
                tenant -> new InMemoryCourseStatsRepository(tenant.tenantId())
        );
    }
}
