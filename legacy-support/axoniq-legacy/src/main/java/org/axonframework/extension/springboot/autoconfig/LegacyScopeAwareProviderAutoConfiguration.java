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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.extension.springboot.LegacyDeadlineProperties;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeAwareProviderSettings;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures the {@link ScopeAwareProviderSettings} of the configuration's {@link ScopeAwareProvider} from the
 * {@link LegacyDeadlineProperties}.
 * <p>
 * The provider itself is registered by the
 * {@link org.axonframework.messaging.LegacyScopeAwareProviderConfigurationEnhancer}, with and without Spring.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@AutoConfiguration
@ConditionalOnClass(ScopeAwareProviderSettings.class)
@EnableConfigurationProperties(LegacyDeadlineProperties.class)
public class LegacyScopeAwareProviderAutoConfiguration {

    /**
     * Creates the settings of the configuration's {@link ScopeAwareProvider}, unless the application defines them.
     *
     * @param properties the deadline properties holding the provider's readiness timeout
     * @return the settings of the configuration's {@link ScopeAwareProvider}
     */
    @Bean
    @ConditionalOnMissingBean
    public ScopeAwareProviderSettings scopeAwareProviderSettings(LegacyDeadlineProperties properties) {
        return new ScopeAwareProviderSettings(properties.getScopeAwareProviderReadinessTimeout());
    }
}
