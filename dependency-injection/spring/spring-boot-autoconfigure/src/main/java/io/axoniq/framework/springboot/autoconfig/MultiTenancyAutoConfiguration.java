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

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

/**
 * Spring Boot auto-configuration wiring multi-tenancy support into Spring Boot applications.
 * <p>
 * Multi-tenancy is active by default: with the {@code axoniq-multi-tenancy} module on the classpath, tenants are
 * discovered from the connected Axon Server contexts and the tenant of each message is resolved automatically. As
 * tenants are Axon Server contexts, multi-tenancy is only activated while Axon Server is enabled: it steps aside when
 * {@code axon.axonserver.enabled=false}, and is switched off entirely with {@code axon.multitenancy.enabled=false}.
 * <p>
 * Application-specific tenant-scoped components need no manual registration: every Spring bean of type
 * {@link TenantComponentProvider} is picked up as an Axon component, subscribed to the tenant lifecycle, and made
 * resolvable as a message-handler parameter. Declaring such a bean is therefore enough to expose a per-tenant resource
 * to handlers. Tenant discovery and resolution are customized the same way, by declaring a
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantResolver} or
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate} bean.
 * <p>
 * The {@link MultiTenancyConfigurationDefaults} and {@link AxonServerMultiTenancyConfigurationDefaults} enhancers
 * are contributed by the {@code axoniq-multi-tenancy} module through the {@link java.util.ServiceLoader}, so this
 * autoconfiguration does not register them. It only contributes the {@link ConfigurationEnhancer} that switches the
 * enhancer on by default, keeping the multi-tenancy core module free of Spring dependencies.
 *
 * @author Jan Galinski
 * @author Laura Devriendt
 * @since 5.3.0
 */
@AutoConfiguration
@ConditionalOnClass(MultiTenancyConfigurationDefaults.class)
public class MultiTenancyAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenancyAutoConfiguration.class);

    /**
     * The order of the enable and disable enhancers, chosen to run just before
     * {@link MultiTenancyConfigurationDefaults}. Enabling must be in place before that enhancer checks for it, and
     * disabling must be in place before it would otherwise run.
     */
    private static final int MULTI_TENANCY_ENHANCER_ORDER = MultiTenancyConfigurationDefaults.ENHANCER_ORDER - 1;

    /**
     * Enables multi-tenancy when the feature is active, so the {@link MultiTenancyConfigurationDefaults} and
     * {@link AxonServerMultiTenancyConfigurationDefaults} enhancer contributed through the
     * {@link java.util.ServiceLoader} take effect.
     * <p>
     * Only contributed while both {@code axon.multitenancy.enabled} and {@code axon.axonserver.enabled} are enabled or
     * absent. Gating on Axon Server keeps multi-tenancy inactive where it cannot function, since tenants are Axon
     * Server contexts.
     *
     * @return a configuration enhancer that enables multi-tenancy
     */
    @Bean
    @ConditionalOnProperty(
            name = {"axon.multitenancy.enabled", "axon.axonserver.enabled"},
            matchIfMissing = true
    )
    public ConfigurationEnhancer enableMultiTenancyConfigurationEnhancer() {
        // TODO flip that to remove the relevant enhancers when implementing https://github.com/AxonIQ/axoniq-framework/issues/258
        return orderedEnhancer(MultiTenancyEnabled::enableMultiTenancyEnhancer);
    }

    /**
     * Warns when multi-tenancy is explicitly enabled while Axon Server is disabled, a combination in which
     * multi-tenancy cannot function and therefore stays inactive, so the misconfiguration is not silent.
     *
     * @return an {@link InitializingBean} logging the misconfiguration on startup
     */
    @Bean
    @ConditionalOnProperty(name = "axon.multitenancy.enabled", havingValue = "true")
    @ConditionalOnProperty(name = "axon.axonserver.enabled", havingValue = "false")
    public InitializingBean multiTenancyRequiresAxonServerWarning() {
        return () -> logger.warn(
                "Multi-tenancy is enabled through 'axon.multitenancy.enabled=true' while Axon Server is disabled "
                        + "through 'axon.axonserver.enabled=false'. Tenants are Axon Server contexts, so multi-tenancy "
                        + "stays inactive. Enable Axon Server, or remove 'axon.multitenancy.enabled=true'.");
    }

    private static ConfigurationEnhancer orderedEnhancer(Consumer<ComponentRegistry> registryAction) {
        return new ConfigurationEnhancer() {
            @Override
            public void enhance(ComponentRegistry registry) {
                registryAction.accept(registry);
            }

            @Override
            public int order() {
                return MULTI_TENANCY_ENHANCER_ORDER;
            }
        };
    }
}
