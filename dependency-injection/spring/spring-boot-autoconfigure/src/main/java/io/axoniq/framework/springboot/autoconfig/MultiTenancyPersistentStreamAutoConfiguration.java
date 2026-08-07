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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSource;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.NoneNestedConditions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

/**
 * Spring Boot auto-configuration making persistent streams multi-tenant, by replacing the
 * {@link PersistentStreamEventSourceFactory} with one that builds a {@link MultiTenantPersistentStreamEventSource} per
 * configured stream.
 * <p>
 * Nothing else changes for an application: streams stay configured under {@code axon.axonserver.persistent-streams} and
 * are still referenced by {@code axon.eventhandling.processors.<name>.source}, but each one is now consumed from every
 * tenant's Axon Server context, and every event reaches its handlers labelled with the tenant it belongs to. A projection
 * therefore lands in the right tenant's data source as soon as its handler takes a
 * {@link io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped} parameter.
 * <p>
 * Runs before {@link PersistentStreamAutoConfiguration}, whose {@code @ConditionalOnMissingBean} factory then backs off.
 * Declaring a {@link PersistentStreamEventSourceFactory} bean of your own takes precedence over both, which is the way to
 * control how each tenant's stream is built.
 * <p>
 * Contributes the factory only while multi-tenancy applies, reusing the condition
 * {@link MultiTenancyAutoConfiguration} switches itself off by, so it steps aside for exactly the reasons multi-tenancy
 * itself is inactive. On top of that it needs persistent streams to exist at all, which is what
 * {@code axon.axonserver.event-store.enabled} decides, the same property {@link PersistentStreamAutoConfiguration} gates
 * its own beans on.
 *
 * @author Jakob Hatzl
 * @see MultiTenantPersistentStreamEventSourceFactory
 * @since 5.3.0
 */
@AutoConfiguration(before = PersistentStreamAutoConfiguration.class)
@ConditionalOnClass(MultiTenancyConfigurationDefaults.class)
public class MultiTenancyPersistentStreamAutoConfiguration {

    /**
     * Creates the {@link PersistentStreamEventSourceFactory} building a per-tenant fan-out for every configured
     * persistent stream.
     *
     * @return the multi-tenant {@link PersistentStreamEventSourceFactory}
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(MultiTenancyApplies.class)
    @ConditionalOnProperty(name = "axon.axonserver.event-store.enabled", matchIfMissing = true)
    public PersistentStreamEventSourceFactory multiTenantPersistentStreamEventSourceFactory() {
        return new MultiTenantPersistentStreamEventSourceFactory();
    }

    /**
     * Matches when multi-tenancy applies, the inverse of
     * {@link MultiTenancyAutoConfiguration.MultiTenancyDoesNotApply}.
     * <p>
     * Delegates to that condition rather than restating the properties it reads, so this factory is contributed for
     * exactly as long as multi-tenancy is active. {@link MultiTenancyAutoConfiguration} needs only the negative form,
     * since all it contributes is the enhancer switching multi-tenancy off, so the positive one lives here with the bean
     * that needs it.
     */
    static class MultiTenancyApplies extends NoneNestedConditions {

        MultiTenancyApplies() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @Conditional(MultiTenancyAutoConfiguration.MultiTenancyDoesNotApply.class)
        private static final class MultiTenancyIsInactive {}
    }
}
