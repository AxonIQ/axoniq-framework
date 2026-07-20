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
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MultiTenancyAutoConfiguration}.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
class MultiTenancyAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MultiTenancyAutoConfiguration.class));

    @Nested
    class WhenAutoConfigured {

        @Test
        void registersTheDefaultMultiTenancyEnhancer() {
            contextRunner.run(context -> {
                assertThat(context).hasSingleBean(MultiTenancyConfigurationDefaults.class);
                assertThat(context).hasSingleBean(ConfigurationEnhancer.class);
            });
        }
    }

    @Nested
    class WhenDisabled {

        @Test
        void registersTheDisableEnhancerInsteadOfTheDefaultOne() {
            contextRunner.withPropertyValues("axon.multitenancy.enabled=false")
                         .run(context -> {
                             assertThat(context).doesNotHaveBean(MultiTenancyConfigurationDefaults.class);
                             assertThat(context).hasSingleBean(ConfigurationEnhancer.class);
                         });
        }
    }

    @Nested
    class WhenCustomEnhancerIsProvided {

        @Test
        void keepsTheUserProvidedEnhancer() {
            MultiTenancyConfigurationDefaults customEnhancer = new MultiTenancyConfigurationDefaults();

            contextRunner.withBean(MultiTenancyConfigurationDefaults.class, () -> customEnhancer)
                         .run(context -> {
                             assertThat(context).hasSingleBean(MultiTenancyConfigurationDefaults.class);
                             assertThat(context.getBean(MultiTenancyConfigurationDefaults.class)).isSameAs(customEnhancer);
                         });
        }
    }
}
