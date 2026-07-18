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

import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for the Axoniq Framework multi-tenancy enhancer.
 * <p>
 * The multi-tenancy core module remains free of direct Spring dependencies. This auto-configuration only contributes
 * the {@link MultiTenancyConfigurationDefaults} enhancer as a Spring bean so the existing Spring-based Axon setup can
 * pick it up automatically.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
@AutoConfiguration
public class MultiTenancyAutoConfiguration {

    /**
     * Creates the default multi-tenancy enhancer when no custom one is provided.
     *
     * @return the default multi-tenancy configuration enhancer
     */
    @Bean
    @ConditionalOnProperty(name = "axon.multitenancy.enabled", matchIfMissing = true)
    @ConditionalOnMissingBean(MultiTenancyConfigurationDefaults.class)
    public MultiTenancyConfigurationDefaults multiTenancyConfigurationDefaults() {
        return new MultiTenancyConfigurationDefaults();
    }

    /**
     * Disables the multi-tenancy enhancer when the feature is switched off through Spring Boot properties.
     *
     * @return a configuration enhancer that disables multi-tenancy configuration
     */
    @Bean
    @ConditionalOnProperty(name = "axon.multitenancy.enabled", havingValue = "false")
    public ConfigurationEnhancer disableMultiTenancyConfigurationEnhancer() {
        return new ConfigurationEnhancer() {
            @Override
            public void enhance(ComponentRegistry registry) {
                registry.disableEnhancer(MultiTenancyConfigurationDefaults.class);
            }

            @Override
            public int order() {
                return Integer.MIN_VALUE;
            }
        };
    }
}
